import json
import os
import uuid
import datetime
from typing import Annotated, Optional
from contextlib import asynccontextmanager
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, status, Header, Depends
from pydantic import BaseModel, Field
from sqlalchemy import Column, String, Integer, DateTime, Text, select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

# Database configuration (Production PostgreSQL asyncpg with env fallback)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://salestorm_user:secure_password@localhost:5432/salestorm_db"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
Base = declarative_base()

# ---------------------------------------------------------
# Database Models: Payment and Transactional Outbox
# ---------------------------------------------------------
class PaymentRecord(Base):
    __tablename__ = "payments"

    id = Column(String(64), primary_key=True)
    idempotency_key = Column(String(128), unique=True, index=True, nullable=False)
    reservation_id = Column(String(64), nullable=False, index=True)
    user_id = Column(String(64), nullable=False)
    amount_cents = Column(Integer, nullable=False)
    currency = Column(String(3), default="USD")
    status = Column(String(32), nullable=False)  # SUCCESS, FAILED
    gateway_transaction_id = Column(String(128), nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    event_id = Column(String(64), primary_key=True)
    aggregate_type = Column(String(64), nullable=False, default="PAYMENT")
    aggregate_id = Column(String(64), nullable=False)
    event_type = Column(String(64), nullable=False)  # ORDER_PAID, PAYMENT_FAILED
    payload = Column(Text, nullable=False)           # Serialized JSON
    status = Column(String(32), nullable=False, default="PENDING")  # PENDING, PROCESSED, FAILED
    retry_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)

# ---------------------------------------------------------
# Redis and Gateway Helpers
# ---------------------------------------------------------
redis_client: aioredis.Redis | None = None

async def init_redis():
    global redis_client
    if redis_client is None:
        try:
            client = aioredis.from_url(REDIS_URL, decode_responses=True)
            await client.ping()
            redis_client = client
        except Exception:
            import fakeredis.aioredis as fakeredis
            redis_client = fakeredis.FakeRedis(decode_responses=True)
    return redis_client

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_redis()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    if redis_client:
        await redis_client.aclose()
    await engine.dispose()

app = FastAPI(title="SALESTORM Payment & Outbox Service", lifespan=lifespan)

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

class PaymentRequest(BaseModel):
    reservation_id: str = Field(..., example="res_98fbc12a")
    user_id: str = Field(..., example="usr_12345")
    amount_cents: int = Field(..., example=99900)
    currency: str = Field(default="USD", example="USD")

class PaymentResponse(BaseModel):
    payment_id: str
    idempotency_key: str
    status: str
    gateway_transaction_id: str
    cached_response: bool = False

# ---------------------------------------------------------
# Simulated Payment Gateway (Stripe/Razorpay with Idempotency)
# ---------------------------------------------------------
async def call_external_gateway(amount: int, currency: str, gateway_idemp_key: str) -> dict:
    """
    Simulates remote gateway integration.
    Strictly forwards the idempotency key to prevent double charge at the provider.
    """
    return {
        "gateway_transaction_id": f"ch_{uuid.uuid4().hex[:14]}",
        "captured": True,
        "amount": amount,
        "currency": currency,
        "provider_idempotency_received": gateway_idemp_key
    }

# ---------------------------------------------------------
# Payment API with Idempotency & Transactional Outbox
# ---------------------------------------------------------
@app.post(
    "/api/v1/payments/charge",
    response_model=PaymentResponse,
    status_code=status.HTTP_200_OK
)
async def process_payment(
    payload: PaymentRequest,
    x_idempotency_key: Annotated[str, Header()],
    db: AsyncSession = Depends(get_db)
):
    redis = await init_redis()
    idemp_cache_key = f"payment_resp:{x_idempotency_key}"
    in_flight_lock_key = f"lock:payment:{x_idempotency_key}"

    # 1. Fast Cache Check: Return completed transaction response if already processed
    cached_payload = await redis.get(idemp_cache_key)
    if cached_payload:
        data = json.loads(cached_payload)
        data["cached_response"] = True
        return PaymentResponse(**data)

    # 2. Acquire Distributed In-Flight Lock (30-second lease to prevent parallel double charges)
    acquired = await redis.set(in_flight_lock_key, "PROCESSING", nx=True, ex=30)
    if not acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A payment request with this idempotency key is currently processing. Retry shortly."
        )

    try:
        # 3. Database Check: Guard against cache eviction
        existing_payment = await db.scalar(
            select(PaymentRecord).where(PaymentRecord.idempotency_key == x_idempotency_key)
        )
        if existing_payment:
            resp_dict = {
                "payment_id": existing_payment.id,
                "idempotency_key": existing_payment.idempotency_key,
                "status": existing_payment.status,
                "gateway_transaction_id": existing_payment.gateway_transaction_id,
                "cached_response": True
            }
            # Re-seed cache
            await redis.set(idemp_cache_key, json.dumps(resp_dict), ex=86400)
            return PaymentResponse(**resp_dict)

        # 4. Invoke External Payment Gateway with Idempotency Token
        gateway_result = await call_external_gateway(
            amount=payload.amount_cents,
            currency=payload.currency,
            gateway_idemp_key=x_idempotency_key
        )

        payment_id = f"pay_{uuid.uuid4().hex[:12]}"
        event_id = f"evt_{uuid.uuid4().hex[:12]}"

        # 5. ATOMIC TRANSACTION: Write Payment Record AND Outbox Event inside the same DB transaction
        try:
            # A. Record the successful payment
            payment_record = PaymentRecord(
                id=payment_id,
                idempotency_key=x_idempotency_key,
                reservation_id=payload.reservation_id,
                user_id=payload.user_id,
                amount_cents=payload.amount_cents,
                currency=payload.currency,
                status="SUCCESS",
                gateway_transaction_id=gateway_result["gateway_transaction_id"]
            )
            db.add(payment_record)

            # B. Record Outbox Event for guaranteed downstream delivery (Order Service)
            order_paid_event = {
                "event_id": event_id,
                "payment_id": payment_id,
                "reservation_id": payload.reservation_id,
                "user_id": payload.user_id,
                "amount_cents": payload.amount_cents,
                "currency": payload.currency,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            outbox_record = OutboxEvent(
                event_id=event_id,
                aggregate_type="PAYMENT",
                aggregate_id=payment_id,
                event_type="ORDER_PAID",
                payload=json.dumps(order_paid_event),
                status="PENDING",
                retry_count=0
            )
            db.add(outbox_record)
            await db.commit()
        except Exception:
            await db.rollback()
            raise

        response_payload = {
            "payment_id": payment_id,
            "idempotency_key": x_idempotency_key,
            "status": "SUCCESS",
            "gateway_transaction_id": gateway_result["gateway_transaction_id"],
            "cached_response": False
        }

        # 6. Cache Response for 24 hours to serve subsequent retries instantly
        await redis.set(idemp_cache_key, json.dumps(response_payload), ex=86400)
        return PaymentResponse(**response_payload)

    finally:
        # 7. Release in-flight processing lock
        await redis.delete(in_flight_lock_key)
