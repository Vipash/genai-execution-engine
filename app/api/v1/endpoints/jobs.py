from uuid import UUID
from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import get_db
from app.models.job import Job
from app.schemas.job import JobCreateRequest, JobResponse

router = APIRouter()

@router.post("/", response_model=JobResponse, status_code=status.HTTP_202_ACCEPTED)
async def submit_job(
    request: JobCreateRequest,
    x_idempotency_key: str = Header(..., description="Unique client idempotency key"),
    db: AsyncSession = Depends(get_db)
):
    # Skeleton placeholder - Phase 2 will implement Redis NX PX guard and Outbox insert
    stmt = select(Job).where(Job.idempotency_key == x_idempotency_key)
    result = await db.execute(stmt)
    existing_job = result.scalar_one_or_none()
    
    if existing_job:
        return existing_job
        
    new_job = Job(
        idempotency_key=x_idempotency_key,
        workflow_type=request.workflow_type,
        payload=request.payload,
        status="queued"
    )
    db.add(new_job)
    await db.commit()
    await db.refresh(new_job)
    return new_job

@router.get("/{job_id}", response_model=JobResponse)
async def get_job_status(job_id: UUID, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == job_id)
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
