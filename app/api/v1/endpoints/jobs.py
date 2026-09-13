from uuid import UUID
from fastapi import APIRouter, Depends, Header, HTTPException, status
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.db import get_db
from app.core.redis import get_redis
from app.models.job import Job
from app.models.outbox import OutboxEvent
from app.schemas.job import JobCreateRequest, JobResponse
from app.services.idempotency import IdempotencyService

router = APIRouter()

@router.post("/", response_model=JobResponse, status_code=status.HTTP_202_ACCEPTED)
async def submit_job(
    request: JobCreateRequest,
    x_idempotency_key: str = Header(..., description="Client idempotency key UUID"),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    idempotency_svc = IdempotencyService(redis, db)
    
    # Check/Reserve idempotency key
    action, existing_job_id = await idempotency_svc.reserve_or_get(x_idempotency_key)
    
    if action == "CONFLICT":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A request with this idempotency key is currently processing."
        )

    if action == "EXISTING":
        stmt = (
            select(Job)
            .where(Job.id == existing_job_id)
            .options(selectinload(Job.steps))
        )
        result = await db.execute(stmt)
        return result.scalar_one()

    # Create Job and Outbox Event within an atomic transaction
    try:
        new_job = Job(
            idempotency_key=x_idempotency_key,
            workflow_type=request.workflow_type,
            payload=request.payload,
            status="queued"
        )
        db.add(new_job)
        await db.flush()  # Populates new_job.id without committing

        # Prepare transactional outbox record
        outbox_event = OutboxEvent(
            event_type="job.created",
            stream_name="jobs:stream",
            payload={
                "job_id": str(new_job.id),
                "workflow_type": new_job.workflow_type,
                "payload": new_job.payload
            },
            status="pending"
        )
        db.add(outbox_event)
        
        # Commit both atomically
        await db.commit()
        await db.refresh(new_job, attribute_names=["steps"])

        # Finalize idempotency state in Redis
        await idempotency_svc.finalize(x_idempotency_key, new_job.id)
        
        return new_job

    except Exception:
        await db.rollback()
        await idempotency_svc.release(x_idempotency_key)
        raise

@router.get("/{job_id}", response_model=JobResponse)
async def get_job_status(job_id: UUID, db: AsyncSession = Depends(get_db)):
    stmt = (
        select(Job)
        .where(Job.id == job_id)
        .options(selectinload(Job.steps))
    )
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job