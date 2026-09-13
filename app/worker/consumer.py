"""
Redis Streams Consumer Group Worker with Heartbeats and Self-PEL Drain.
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

        # 1. Start background Heartbeat task
        heartbeat = HeartbeatService(redis, self.consumer_id)
        heartbeat_task = asyncio.create_task(heartbeat.run(self.stop_event))

        try:
            while not self.stop_event.is_set():
                try:
                    # First priority: Drain any pending messages previously assigned to this worker ('0')
                    pending_streams = await redis.xreadgroup(
                        groupname=GROUP_NAME,
                        consumername=self.consumer_id,
                        streams={STREAM_NAME: "0"},
                        count=1
                    )
                    
                    messages_to_process = []
                    if pending_streams and pending_streams[0][1]:
                        messages_to_process = pending_streams[0][1]
                    else:
                        # Second priority: Read new incoming messages ('>')
                        streams = await redis.xreadgroup(
                            groupname=GROUP_NAME,
                            consumername=self.consumer_id,
                            streams={STREAM_NAME: ">"},
                            count=1,
                            block=2000
                        )
                        if streams and streams[0][1]:
                            messages_to_process = streams[0][1]

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

    async def _process_message(self, redis: Redis, msg_id: str, raw_data: dict[str, str]) -> None:
        payload_json = raw_data.get("payload")
        if not payload_json:
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
            stmt = select(Job).where(Job.id == job_uuid)
            res = await session.execute(stmt)
            job = res.scalar_one_or_none()

            if not job:
                logger.warn("worker.job_not_found", job_id=job_id_str)
                await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                return

            if job.status in ("succeeded", "failed", "dead_lettered"):
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