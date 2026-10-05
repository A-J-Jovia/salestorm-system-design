import asyncio
import datetime
import json
import logging
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from payment_service import DATABASE_URL, OutboxEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [OutboxWorker] %(message)s"
)
logger = logging.getLogger("OutboxRecoveryWorker")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

MAX_RETRIES = 5
BATCH_SIZE = 50
POLL_INTERVAL_SECONDS = 2.0

class SimulatedMessageBroker:
    """
    Simulates Kafka/RabbitMQ client with configurable simulated outage flag
    to demonstrate transactional recovery.
    """
    def __init__(self, simulate_outage: bool = False):
        self.simulate_outage = simulate_outage
        self.published_events = []

    async def publish(self, topic: str, event_payload: dict) -> bool:
        if self.simulate_outage:
            logger.warning(f"Broker down! Failed publishing to topic: {topic}")
            raise ConnectionError("Broker connection timed out (Simulated Catastrophic Outage)")
        
        # Successful delivery confirmation
        self.published_events.append({"topic": topic, "payload": event_payload})
        logger.info(f"Published to topic '{topic}': event_id={event_payload.get('event_id')}")
        return True

async def process_outbox_batch(broker: SimulatedMessageBroker, max_batch: int = BATCH_SIZE) -> int:
    """
    Pulls unacknowledged pending events, publishes them to broker, and marks them PROCESSED.
    Guarantees at-least-once delivery semantics.
    """
    async with AsyncSessionLocal() as session:
        # 1. Fetch pending events (in PostgreSQL, use .with_for_update(skip_locked=True))
        query = (
            select(OutboxEvent)
            .where(OutboxEvent.status == "PENDING")
            .where(OutboxEvent.retry_count < MAX_RETRIES)
            .order_by(OutboxEvent.created_at.asc())
            .limit(max_batch)
        )
        result = await session.execute(query)
        events = result.scalars().all()

        if not events:
            return 0

        logger.info(f"Discovered {len(events)} pending outbox events to reconcile.")
        processed_count = 0

        for event in events:
            try:
                payload = json.loads(event.payload)
                topic = f"salestorm.events.{event.event_type.lower()}"

                # 2. Publish to Message Broker
                await broker.publish(topic=topic, event_payload=payload)

                # 3. Mark PROCESSED on successful publication
                event.status = "PROCESSED"
                event.processed_at = datetime.datetime.utcnow()
                processed_count += 1

            except Exception as exc:
                event.retry_count += 1
                logger.error(
                    f"Delivery failed for event_id={event.event_id} (Attempt {event.retry_count}/{MAX_RETRIES}): {exc}"
                )
                if event.retry_count >= MAX_RETRIES:
                    logger.critical(f"Event {event.event_id} exceeded MAX_RETRIES. Moving to DEAD_LETTER.")
                    event.status = "FAILED"

        # Commit batch status updates
        await session.commit()
        return processed_count

async def start_recovery_worker(broker: SimulatedMessageBroker, single_run: bool = False):
    """Continuous polling loop for outbox recovery."""
    logger.info("Outbox Reconciliation Worker initialized and polling...")
    while True:
        try:
            count = await process_outbox_batch(broker)
            if count > 0:
                logger.info(f"Successfully published and reconciled {count} events.")
        except Exception as e:
            logger.error(f"Error during reconciliation cycle: {e}")

        if single_run:
            break
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    broker = SimulatedMessageBroker(simulate_outage=False)
    asyncio.run(start_recovery_worker(broker, single_run=True))
