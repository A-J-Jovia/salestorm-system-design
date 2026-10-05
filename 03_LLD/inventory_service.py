import uuid
from typing import Annotated
from contextlib import asynccontextmanager
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, status, Header
from pydantic import BaseModel, Field

REDIS_URL = "redis://localhost:6379/0"
redis_client: aioredis.Redis | None = None
script_sha: str | None = None

RESERVE_STOCK_LUA = """
local existing_reservation = redis.call("GET", KEYS[3])
if existing_reservation then
    return -1
end

local current_stock = redis.call("GET", KEYS[1])
if not current_stock then
    return -2
end

current_stock = tonumber(current_stock)
local qty = tonumber(ARGV[1])

if current_stock < qty then
    return 0
end

redis.call("DECRBY", KEYS[1], qty)

redis.call("HMSET", KEYS[2],
    "reservation_id", ARGV[2],
    "user_id", ARGV[4],
    "qty", qty,
    "status", "PENDING_PAYMENT"
)
redis.call("EXPIRE", KEYS[2], tonumber(ARGV[3]))
redis.call("SET", KEYS[3], ARGV[2], "EX", tonumber(ARGV[3]))

return 1
"""

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, script_sha
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True, max_connections=100)
    script_sha = await redis_client.script_load(RESERVE_STOCK_LUA)
    yield
    await redis_client.aclose()

app = FastAPI(title="SALESTORM Inventory Gatekeeper", lifespan=lifespan)

class ReservationRequest(BaseModel):
    item_id: str = Field(..., example="item_1001")
    user_id: str = Field(..., example="usr_789")
    quantity: int = Field(default=1, ge=1, le=2)

class ReservationResponse(BaseModel):
    status: str
    reservation_id: str
    item_id: str
    reserved_quantity: int
    ttl_seconds: int

@app.post(
    "/api/v1/inventory/reserve",
    response_model=ReservationResponse,
    status_code=status.HTTP_201_CREATED
)
async def reserve_stock(
    payload: ReservationRequest,
    x_idempotency_key: Annotated[str, Header()]
):
    reservation_id = f"res_{uuid.uuid4().hex[:12]}"
    ttl_seconds = 300

    stock_key = f"stock:{payload.item_id}"
    reservation_key = f"reservation:{reservation_id}"
    idemp_key = f"idemp:{payload.user_id}:{x_idempotency_key}"

    keys = [stock_key, reservation_key, idemp_key]
    args = [payload.quantity, reservation_id, ttl_seconds, payload.user_id]

    try:
        result = await redis_client.evalsha(script_sha, len(keys), *keys, *args)
    except aioredis.exceptions.NoScriptError:
        result = await redis_client.eval(RESERVE_STOCK_LUA, len(keys), *keys, *args)

    if result == 1:
        return ReservationResponse(
            status="RESERVED",
            reservation_id=reservation_id,
            item_id=payload.item_id,
            reserved_quantity=payload.quantity,
            ttl_seconds=ttl_seconds
        )
    elif result == 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Item is out of stock. Zero available inventory."
        )
    elif result == -1:
        existing_res_id = await redis_client.get(idemp_key)
        return ReservationResponse(
            status="ALREADY_RESERVED",
            reservation_id=existing_res_id,
            item_id=payload.item_id,
            reserved_quantity=payload.quantity,
            ttl_seconds=ttl_seconds
        )
    elif result == -2:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product catalog entry not found."
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected reservation error."
        )
