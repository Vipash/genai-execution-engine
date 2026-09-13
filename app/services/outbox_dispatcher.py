"""
Transactional Outbox Dispatcher.
Continuously publishes pending events from PostgreSQL to Redis Streams.
"""
import asyncio
import json
import structlog
from sqlalchemy import select

from app.core.db import async_session_factory
from app.core.redis import get_redis
from app.models.outbox import OutboxEvent

logger = structlog.get_logger(__name__)


class OutboxDispatcher:
    def __init__(self, poll_interval: float = 1.0, batch_size: int = 50):
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self._running = False

    async def run(self, stop_event: asyncio.Event) -> None:
        self._running = True
        logger.info("outbox_dispatcher.started")
        redis_client = get_redis()

        try:
            while not stop_event.is_set():
                processed_count = await self._dispatch_batch(redis_client)

                # If no records were processed, sleep to prevent CPU spinning and DB starvation
                if processed_count == 0:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=self.poll_interval)
                    except asyncio.TimeoutError:
                        pass
        finally:
            logger.info("outbox_dispatcher.stopped")

    async def _dispatch_batch(self, redis_client) -> int:
        async with async_session_factory() as session:
            try:
                # Use SKIP LOCKED to support multiple horizontal dispatcher replicas
                stmt = (
                    select(OutboxEvent)
                    .where(OutboxEvent.status == "pending")
                    .order_by(OutboxEvent.created_at.asc())
                    .limit(self.batch_size)
                    .with_for_update(skip_locked=True)
                )
                result = await session.execute(stmt)
                events = list(result.scalars().all())

                if not events:
                    return 0

                for event in events:
                    stream_data = {
                        "event_id": str(event.id),
                        "event_type": event.event_type,
                        "payload": json.dumps(event.payload),
                    }
                    await redis_client.xadd(
                        name=event.stream_name,
                        fields=stream_data,
                        maxlen=100_000,
                        approximate=True,
                    )
                    event.status = "published"

                await session.commit()
                logger.info("outbox_dispatcher.batch_published", count=len(events))
                return len(events)

            except Exception as exc:
                await session.rollback()
                logger.error("outbox_dispatcher.error", error=str(exc))
                # Sleep briefly on error to avoid continuous spinning on DB errors
                await asyncio.sleep(2.0)
                return 0