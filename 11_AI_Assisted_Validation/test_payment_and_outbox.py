import asyncio
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../03_LLD")))
from httpx import AsyncClient, ASGITransport
from payment_service import app, engine, Base, AsyncSessionLocal, PaymentRecord, OutboxEvent
from outbox_recovery_worker import SimulatedMessageBroker, process_outbox_batch
from sqlalchemy import select

async def run_validation_suite():
    print("=================================================================")
    print("SALESTORM: PAYMENT IDEMPOTENCY & OUTBOX RECOVERY VALIDATION")
    print("=================================================================")

    # 1. Reset database tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # -------------------------------------------------------------
        # TEST 1: Payment Idempotency & Duplicate Client Submissions
        # -------------------------------------------------------------
        print("\n--- TEST 1: Idempotent Retries & Duplicate Prevention ---")
        idemp_key = "idemp_test_user_42_attempt_1"
        payload = {
            "reservation_id": "res_macbook_01",
            "user_id": "usr_42",
            "amount_cents": 199900,
            "currency": "USD"
        }

        # First request (normal charge)
        resp1 = await client.post(
            "/api/v1/payments/charge",
            json=payload,
            headers={"x-idempotency-key": idemp_key}
        )
        assert resp1.status_code == 200, f"Expected 200, got {resp1.status_code}"
        data1 = resp1.json()
        print(f"Initial Payment Succeeded: ID={data1['payment_id']}, Cached={data1['cached_response']}")
        assert data1["cached_response"] is False

        # Second request (simulating network timeout retry with identical key)
        resp2 = await client.post(
            "/api/v1/payments/charge",
            json=payload,
            headers={"x-idempotency-key": idemp_key}
        )
        assert resp2.status_code == 200, f"Expected 200, got {resp2.status_code}"
        data2 = resp2.json()
        print(f"Retry Payment Response  : ID={data2['payment_id']}, Cached={data2['cached_response']}")
        
        # Verify no double charge
        assert data2["payment_id"] == data1["payment_id"]
        assert data2["gateway_transaction_id"] == data1["gateway_transaction_id"]
        assert data2["cached_response"] is True

        # Verify only 1 payment row exists in DB
        async with AsyncSessionLocal() as session:
            count = len((await session.scalars(select(PaymentRecord))).all())
            print(f"Total Payments in DB     : {count} (Strict single charge guarantee)")
            assert count == 1, f"Expected exactly 1 payment record, found {count}"

        # -------------------------------------------------------------
        # TEST 2: Transactional Outbox Under Catastrophic Broker Outage
        # -------------------------------------------------------------
        print("\n--- TEST 2: Transactional Outbox During Downstream Outage ---")
        async with AsyncSessionLocal() as session:
            outbox_events = (await session.scalars(select(OutboxEvent))).all()
            print(f"Outbox Records Stored   : {len(outbox_events)}")
            assert len(outbox_events) == 1
            print(f"Initial Outbox Status    : {outbox_events[0].status} (Zero data loss)")
            assert outbox_events[0].status == "PENDING"

        # Simulate broker outage
        print("\nSimulating Downstream Broker Outage (Broker unreachable)...")
        failing_broker = SimulatedMessageBroker(simulate_outage=True)
        reconciled = await process_outbox_batch(failing_broker)
        print(f"Reconciled during outage : {reconciled} events (failed cleanly as expected)")
        assert reconciled == 0

        # Verify record remains safely PENDING
        async with AsyncSessionLocal() as session:
            event = (await session.scalars(select(OutboxEvent))).first()
            print(f"Outbox Status after down: {event.status}, Retry Count: {event.retry_count}")
            assert event.status == "PENDING"
            assert event.retry_count == 1

        # -------------------------------------------------------------
        # TEST 3: Outbox Recovery Worker Reconciles on Service Restoration
        # -------------------------------------------------------------
        print("\n--- TEST 3: Broker Restoration & Eventual Consistency ---")
        recovered_broker = SimulatedMessageBroker(simulate_outage=False)
        reconciled_after_recovery = await process_outbox_batch(recovered_broker)
        print(f"Reconciled after recovery: {reconciled_after_recovery} events")
        assert reconciled_after_recovery == 1

        # Verify outbox event is now marked PROCESSED
        async with AsyncSessionLocal() as session:
            event = (await session.scalars(select(OutboxEvent))).first()
            print(f"Final Outbox Status     : {event.status}")
            print(f"Processed Timestamp      : {event.processed_at}")
            assert event.status == "PROCESSED"
            assert event.processed_at is not None

    print("\n[PASSED] All Idempotency, Transactional Outbox, and Recovery assertions verified successfully!")

if __name__ == "__main__":
    asyncio.run(run_validation_suite())
