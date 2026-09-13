"""
Redis Streams Consumer Group Worker.
Processes jobs with at-least-once delivery guarantees and safe transactional updates.
"""
import asyncio
import json
import socket
import uuid
from datetime import datetime, timezone
import structlog
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import select
from app.core.config import settings
from app.core.db import async_session_factory
from app.core.redis import get_redis
from app.models.job import Job, JobAttempt
from app.worker.pipeline import WorkflowPipeline

logger = structlog.get_logger(__name__)

STREAM_NAME = "jobs:stream"
GROUP_NAME = "job_workers"

class StreamWorker:
    def __init__(self, consumer_id: str | None = None):
        # Format: worker-<hostname>-<short-uuid>
        self.consumer_id = consumer_id or f"worker-{socket.gethostname()[:8]}-{uuid.uuid4().hex[:6]}"
        self.stop_event = asyncio.Event()

    async def init_consumer_group(self, redis: Redis) -> None:
        """Ensures the Redis Stream consumer group exists without throwing if already present."""
        try:
            # MKSTREAM creates stream if it doesn't exist yet; id="0" reads from beginning
            await redis.xgroup_create(name=STREAM_NAME, groupname=GROUP_NAME, id="0", mkstream=True)
            logger.info("consumer_group.created", stream=STREAM_NAME, group=GROUP_NAME)
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info("consumer_group.already_exists", stream=STREAM_NAME, group=GROUP_NAME)
            else:
                raise

    async def start(self) -> None:
        logger.info("worker.starting", consumer_id=self.consumer_id)
        redis = get_redis()
        await self.init_consumer_group(redis)

        try:
            while not self.stop_event.is_set():
                try:
                    # Read up to 2 new messages, blocking for max 2000ms
                    # ">" means only messages never delivered to any consumer
                    streams = await redis.xreadgroup(
                        groupname=GROUP_NAME,
                        consumername=self.consumer_id,
                        streams={STREAM_NAME: ">"},
                        count=2,
                        block=2000
                    )

                    if not streams:
                        continue

                    for stream_key, messages in streams:
                        for msg_id, raw_data in messages:
                            await self._process_message(redis, msg_id, raw_data)

                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.error("worker.loop_error", error=str(exc))
                    await asyncio.sleep(1)

        finally:
            await redis.aclose()
            logger.info("worker.stopped", consumer_id=self.consumer_id)

    async def _process_message(self, redis: Redis, msg_id: str, raw_data: dict[str, str]) -> None:
        payload_json = raw_data.get("payload")
        if not payload_json:
            # Poison pill without payload: ACK to discard
            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
            return

        data = json.loads(payload_json)
        job_id_str = data.get("job_id")
        if not job_id_str:
            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
            return

        job_uuid = uuid.UUID(job_id_str)
        logger.info("worker.job_claimed", job_id=job_id_str, msg_id=msg_id)

        async with async_session_factory() as session:
            # 1. Fetch Job
            stmt = select(Job).where(Job.id == job_uuid)
            res = await session.execute(stmt)
            job = res.scalar_one_or_none()

            if not job:
                logger.warn("worker.job_not_found_in_db", job_id=job_id_str)
                await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                return

            # Skip if already marked finished
            if job.status in ("succeeded", "failed", "dead_lettered"):
                await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                return

            # 2. Mark Running & Register Attempt
            job.status = "running"
            if not job.started_at:
                job.started_at = datetime.now(timezone.utc)

            # Count attempts
            attempt_count_stmt = select(JobAttempt).where(JobAttempt.job_id == job.id)
            attempt_res = await session.execute(attempt_count_stmt)
            attempts = list(attempt_res.scalars().all())
            attempt_number = len(attempts) + 1

            attempt = JobAttempt(
                job_id=job.id,
                attempt_number=attempt_number,
                worker_id=self.consumer_id,
                status="running"
            )
            session.add(attempt)
            await session.commit()

            # 3. Execute Pipeline
            pipeline = WorkflowPipeline(session)
            try:
                result_state = await pipeline.execute(job)

                # 4. Success Path: Mark Succeeded and XACK
                job.status = "succeeded"
                job.completed_at = datetime.now(timezone.utc)
                job.result = result_state
                attempt.status = "succeeded"

                await session.commit()
                await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                logger.info("worker.job_completed", job_id=job_id_str, msg_id=msg_id)

            except Exception as e:
                logger.error("worker.job_failed", job_id=job_id_str, error=str(e))
                await session.rollback()
                
                # Re-fetch in new session to record failure
                async with async_session_factory() as err_session:
                    err_attempt = JobAttempt(
                        job_id=job_uuid,
                        attempt_number=attempt_number,
                        worker_id=self.consumer_id,
                        status="failed",
                        error_log=str(e)
                    )
                    err_session.add(err_attempt)
                    
                    if attempt_number >= settings.JOB_MAX_RETRIES:
                        err_stmt = select(Job).where(Job.id == job_uuid)
                        err_res = await err_session.execute(err_stmt)
                        j = err_res.scalar_one()
                        j.status = "failed"
                        j.error_message = f"Exceeded max retries: {str(e)}"
                        # Acknowledge to drop from PEL since DLQ supervisor will handle in Phase 4
                        await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)

                    await err_session.commit()