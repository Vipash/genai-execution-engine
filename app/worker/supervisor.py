"""
Supervisor Engine:
1. Reclaims orphaned tasks using XAUTOCLAIM.
2. Identifies poison pills and routes them to jobs:dlq.
"""

import asyncio
import json
import uuid

import structlog
from redis.asyncio import Redis
from sqlalchemy import select

from app.core.config import settings
from app.core.db import async_session_factory
from app.core.redis import get_redis
from app.models.job import Job

logger = structlog.get_logger(__name__)

STREAM_NAME = "jobs:stream"
GROUP_NAME = "job_workers"
DLQ_STREAM = "jobs:dlq"


class RecoverySupervisor:
    def __init__(self, min_idle_ms: int = 20_000, check_interval: int = 10):
        self.min_idle_ms = min_idle_ms
        self.check_interval = check_interval
        self.supervisor_id = f"supervisor-{uuid.uuid4().hex[:6]}"
        self.stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("supervisor.started", supervisor_id=self.supervisor_id)
        redis = get_redis()

        try:
            while not self.stop_event.is_set():
                await self._scan_and_recover(redis)
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(), timeout=self.check_interval
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            await redis.aclose()
            logger.info("supervisor.stopped")

    async def _scan_and_recover(self, redis: Redis) -> None:
        try:
            # XAUTOCLAIM automatically scans PEL for messages idle > min_idle_ms
            # and claims them to the supervisor to triage.
            # Returns: [next_start_id, [ (msg_id, fields), ... ], [deleted_msg_ids]]
            claim_res = await redis.xautoclaim(
                name=STREAM_NAME,
                groupname=GROUP_NAME,
                consumername=self.supervisor_id,
                min_idle_time=self.min_idle_ms,
                start_id="0-0",
                count=10,
            )

            if not claim_res or len(claim_res) < 2:
                return

            messages = claim_res[1]
            if not messages:
                return

            logger.info("supervisor.orphaned_messages_found", count=len(messages))

            for msg_id, raw_fields in messages:
                await self._triage_orphaned_message(redis, msg_id, raw_fields)

        except Exception as exc: # noqa: BLE001
            logger.error("supervisor.recovery_cycle_error", error=str(exc))

    async def _triage_orphaned_message(
        self, redis: Redis, msg_id: str, fields: dict[str, str]
    ) -> None:
        # Check how many delivery attempts this message has had
        pending_info = await redis.xpending_range(
            name=STREAM_NAME, groupname=GROUP_NAME, min=msg_id, max=msg_id, count=1
        )

        delivery_count = 1
        if pending_info:
            delivery_count = pending_info[0].get("times_delivered", 1)

        payload_json = fields.get("payload", "{}")
        data = json.loads(payload_json)
        job_id_str = data.get("job_id")

        logger.warning(
            "supervisor.triaging_message",
            msg_id=msg_id,
            job_id=job_id_str,
            deliveries=delivery_count,
        )

        if delivery_count > settings.JOB_MAX_RETRIES:
            # Poison pill exceeded max retries: Route to DLQ and mark Failed
            logger.error("supervisor.routing_to_dlq", job_id=job_id_str, msg_id=msg_id)

            # 1. Publish to Dead-Letter Stream
            await redis.xadd(
                DLQ_STREAM,
                {
                    "original_msg_id": msg_id,
                    "reason": "max_retries_exceeded",
                    "payload": payload_json,
                    "failed_at": str(asyncio.get_event_loop().time()),
                },
            )

            # 2. Update Database Record
            if job_id_str:
                async with async_session_factory() as session:
                    stmt = select(Job).where(Job.id == uuid.UUID(job_id_str))
                    res = await session.execute(stmt)
                    job = res.scalar_one_or_none()
                    if job:
                        job.status = "dead_lettered"
                        job.error_message = f"Exceeded max delivery attempts ({delivery_count}). Moved to DLQ."
                        await session.commit()

            # 3. Acknowledge and clear from active stream PEL
            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
        else:
            # Still retryable: Leave claimed or let a healthy worker pick it up
            logger.info(
                "supervisor.job_reclaimed_for_retry", job_id=job_id_str, msg_id=msg_id
            )
