"""
Redis Streams Consumer Group Worker with Distributed Auto-Claim and Checkpoint Resumption.
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
from app.worker.heartbeat import HeartbeatService
from app.worker.pipeline import WorkflowPipeline

logger = structlog.get_logger(__name__)

STREAM_NAME = "jobs:stream"
GROUP_NAME = "job_workers"
DLQ_STREAM = "jobs:dlq"

class StreamWorker:
    def __init__(self, consumer_id: str | None = None):
        self.consumer_id = consumer_id or f"worker-{socket.gethostname()[:8]}-{uuid.uuid4().hex[:6]}"
        self.stop_event = asyncio.Event()

    async def init_consumer_group(self, redis: Redis) -> None:
        try:
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

        heartbeat = HeartbeatService(redis, self.consumer_id, interval=5, ttl=15)
        heartbeat_task = asyncio.create_task(heartbeat.run(self.stop_event))

        try:
            while not self.stop_event.is_set():
                try:
                    messages_to_process = []

                    # 1. Drain own unacknowledged PEL messages
                    pending = await redis.xreadgroup(
                        groupname=GROUP_NAME,
                        consumername=self.consumer_id,
                        streams={STREAM_NAME: "0"},
                        count=1
                    )
                    if pending and pending[0][1]:
                        messages_to_process = pending[0][1]

                    # 2. If nothing pending locally, auto-claim messages abandoned by dead workers (>8s idle)
                    if not messages_to_process:
                        claim_res = await redis.xautoclaim(
                            name=STREAM_NAME,
                            groupname=GROUP_NAME,
                            consumername=self.consumer_id,
                            min_idle_time=8000,
                            start_id="0-0",
                            count=1
                        )
                        if claim_res and len(claim_res) >= 2 and claim_res[1]:
                            messages_to_process = claim_res[1]
                            logger.warn("worker.claimed_abandoned_task", count=len(messages_to_process))

                    # 3. If still nothing, read new messages with block
                    if not messages_to_process:
                        streams = await redis.xreadgroup(
                            groupname=GROUP_NAME,
                            consumername=self.consumer_id,
                            streams={STREAM_NAME: ">"},
                            count=1,
                            block=2000
                        )
                        if streams and streams[0][1]:
                            messages_to_process = streams[0][1]

                    # Process retrieved tasks
                    for msg_id, raw_data in messages_to_process:
                        await self._process_message(redis, msg_id, raw_data)

                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.error("worker.loop_error", error=str(exc))
                    await asyncio.sleep(1)

        finally:
            self.stop_event.set()
            await heartbeat_task
            await redis.aclose()
            logger.info("worker.stopped", consumer_id=self.consumer_id)

    async def _process_message(self, redis: Redis, msg_id: str, raw_data: dict) -> None:
        # Normalize dictionary keys/values to string to prevent bytes vs str lookup mismatch
        normalized_data = {
            (k.decode("utf-8") if isinstance(k, bytes) else k): 
            (v.decode("utf-8") if isinstance(v, bytes) else v) 
            for k, v in raw_data.items()
        }

        payload_json = normalized_data.get("payload")
        if not payload_json:
            logger.warning("worker.malformed_message_missing_payload", msg_id=msg_id, raw_data=normalized_data)
            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
            return

        try:
            data = json.loads(payload_json) if isinstance(payload_json, str) else payload_json
        except json.JSONDecodeError as jde:
            logger.warning("worker.payload_json_decode_error", msg_id=msg_id, error=str(jde), payload=payload_json)
            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
            return

        # Extract job_id from inner payload JSON (with fallback to top-level stream field)
        job_id_str = (data.get("job_id") if isinstance(data, dict) else None) or normalized_data.get("job_id")
        if not job_id_str:
            logger.warning("worker.malformed_message_missing_job_id", msg_id=msg_id, raw_data=normalized_data)
            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
            return

        try:
            job_uuid = uuid.UUID(job_id_str)
        except ValueError:
            logger.warning("worker.invalid_job_id_format", msg_id=msg_id, job_id=job_id_str)
            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
            return

        # Check delivery attempts for poison pill protection
        pending_info = await redis.xpending_range(
            name=STREAM_NAME, groupname=GROUP_NAME, min=msg_id, max=msg_id, count=1
        )
        delivery_count = pending_info[0].get("times_delivered", 1) if pending_info else 1

        if delivery_count > settings.JOB_MAX_RETRIES:
            logger.error("worker.poison_pill_detected", job_id=job_id_str, deliveries=delivery_count)
            await redis.xadd(DLQ_STREAM, {"original_id": msg_id, "job_id": job_id_str, "payload": payload_json})
            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
            async with async_session_factory() as session:
                stmt = select(Job).where(Job.id == job_uuid)
                res = await session.execute(stmt)
                j = res.scalar_one_or_none()
                if j:
                    j.status = "dead_lettered"
                    j.error_message = f"Exceeded max attempts ({delivery_count})"
                    await session.commit()
            return

        logger.info("worker.job_claimed", job_id=job_id_str, msg_id=msg_id, attempt=delivery_count)

        async with async_session_factory() as session:
            stmt = select(Job).where(Job.id == job_uuid)
            res = await session.execute(stmt)
            job = res.scalar_one_or_none()

            if not job or job.status in ("succeeded", "dead_lettered"):
                logger.warning("worker.job_already_terminal_or_missing", job_id=job_id_str, status=getattr(job, "status", "not_found"))
                await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                return

            job.status = "running"
            if not job.started_at:
                job.started_at = datetime.now(timezone.utc)

            # Record attempt
            attempt_stmt = select(JobAttempt).where(JobAttempt.job_id == job.id)
            attempt_res = await session.execute(attempt_stmt)
            attempt_number = len(list(attempt_res.scalars().all())) + 1

            attempt = JobAttempt(
                job_id=job.id,
                attempt_number=attempt_number,
                worker_id=self.consumer_id,
                status="running"
            )
            session.add(attempt)
            await session.commit()

            # Execute pipeline
            pipeline = WorkflowPipeline(session)
            try:
                result_state = await pipeline.execute(job)

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
                
                async with async_session_factory() as err_session:
                    err_attempt = JobAttempt(
                        job_id=job_uuid,
                        attempt_number=attempt_number,
                        worker_id=self.consumer_id,
                        status="failed",
                        error_log=str(e)
                    )
                    err_session.add(err_attempt)
                    await err_session.commit()