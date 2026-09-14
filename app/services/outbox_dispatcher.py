"""
Transactional Outbox Dispatcher with Anti-Entropy State Reconciliation.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone

import structlog
from sqlalchemy import select

from app.core.db import async_session_factory
from app.core.redis import get_redis
from app.models.job import Job
from app.models.outbox import OutboxEvent

logger = structlog.get_logger(__name__)


class OutboxDispatcher:
    def __init__(
        self,
        poll_interval: float = 0.5,
        batch_size: int = 50,
        reconciliation_interval: float = 60.0,
    ):
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.reconciliation_interval = reconciliation_interval
        self._last_reconcile_time = 0.0

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("outbox_dispatcher.started")
        redis_client = get_redis()

        try:
            while not stop_event.is_set():
                # 1. Normal dispatch cycle
                processed_count = await self._dispatch_batch(redis_client)

                # 2. Periodic Anti-Entropy Reconciliation Sweep
                loop_now = asyncio.get_event_loop().time()
                if loop_now - self._last_reconcile_time > self.reconciliation_interval:
                    await self._reconcile_stale_jobs()
                    self._last_reconcile_time = loop_now

                if processed_count == 0:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=self.poll_interval
                        )
                    except asyncio.TimeoutError:
                        pass
        finally:
            await redis_client.aclose()
            logger.info("outbox_dispatcher.stopped")

    async def _dispatch_batch(self, redis_client) -> int:
        async with async_session_factory() as session:
            try:
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
                return len(events)

            except Exception as exc: # noqa: BLE001
                await session.rollback()
                logger.error("outbox_dispatcher.dispatch_error", error=str(exc))
                return 0

    async def _reconcile_stale_jobs(self) -> None:
        """
        Anti-Entropy Sweeper:
        Finds jobs stuck in 'queued' for > 3 minutes (e.g., due to network drops during XADD)
        and resets their outbox status to 'pending' to force re-transmission.
        """
        async with async_session_factory() as session:
            try:
                stale_threshold = datetime.now(timezone.utc) - timedelta(minutes=3)

                # 1. Identify stale queued jobs
                stmt = (
                    select(Job.id)
                    .where(Job.status == "queued", Job.created_at < stale_threshold)
                    .limit(50)
                )
                res = await session.execute(stmt)
                stale_job_ids = list(res.scalars().all())

                if not stale_job_ids:
                    return

                logger.warning(
                    "reconciler.stale_jobs_detected", count=len(stale_job_ids)
                )

                # 2. Reset their outbox events to 'pending' so the dispatcher will re-publish them
                for job_id in stale_job_ids:
                    # Look for outbox events matching this job
                    # OutboxEvent.payload contains {"job_id": str(job_id)}
                    outbox_stmt = select(OutboxEvent).where(
                        OutboxEvent.payload["job_id"].astext == str(job_id)
                    )
                    outbox_res = await session.execute(outbox_stmt)
                    outbox_event = outbox_res.scalar_one_or_none()

                    if outbox_event and outbox_event.status != "pending":
                        outbox_event.status = "pending"
                        logger.info("reconciler.re_enqueued_job", job_id=str(job_id))

                await session.commit()

            except Exception as exc:
                await session.rollback()
                logger.error("reconciler.sweep_error", error=str(exc))
