"""
One-Shot Project Bootstrapper
Distributed GenAI Workflow & Job Execution Platform
"""

from pathlib import Path

FILES = {
    # -------------------------------------------------------------
    # CONFIGURATION & DOCKER
    # -------------------------------------------------------------
    "requirements.txt": """fastapi>=0.115.0
uvicorn[standard]>=0.30.0
pydantic>=2.8.0
pydantic-settings>=2.4.0
sqlalchemy[asyncio]>=2.0.32
asyncpg>=0.29.0
alembic>=1.13.2
redis>=5.0.8
structlog>=24.4.0
httpx>=0.27.0
prometheus-client>=0.20.0
pytest>=8.3.0
pytest-asyncio>=0.23.8
""",
    "docker-compose.yml": """services:
  postgres:
    image: pgvector/pgvector:pg16
    container_name: genai_postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgrespassword
      POSTGRES_DB: genai_engine
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d genai_engine"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7.4-alpine
    container_name: genai_redis
    command: redis-server --appendonly yes --maxmemory-policy noeviction
    ports:
      - "6379:6379"
    volumes:
      - redisdata:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  pgdata:
  redisdata:
""",
    ".env.example": """APP_ENV=development
APP_NAME=GenAI-Execution-Engine
DEBUG=true
PORT=8000

# Database
DATABASE_URL=postgresql+asyncpg://postgres:postgrespassword@localhost:5432/genai_engine

# Redis
REDIS_URL=redis://localhost:6379/0

# Worker Settings
WORKER_CONCURRENCY=5
JOB_MAX_RETRIES=3
""",
    ".env": """APP_ENV=development
APP_NAME=GenAI-Execution-Engine
DEBUG=true
PORT=8000

DATABASE_URL=postgresql+asyncpg://postgres:postgrespassword@localhost:5432/genai_engine
REDIS_URL=redis://localhost:6379/0

WORKER_CONCURRENCY=5
JOB_MAX_RETRIES=3
""",
    ".gitignore": """__pycache__/
*.py[cod]
*$py.class
.env
.venv/
env/
venv/
*.log
.pytest_cache/
.coverage
htmlcov/
dist/
build/
""",
    # -------------------------------------------------------------
    # APPLICATION CORE
    # -------------------------------------------------------------
    "app/__init__.py": "",
    "app/core/__init__.py": "",
    "app/core/config.py": """from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    APP_ENV: str = "development"
    APP_NAME: str = "GenAI-Execution-Engine"
    DEBUG: bool = True
    PORT: int = 8000

    DATABASE_URL: str
    REDIS_URL: str

    WORKER_CONCURRENCY: int = 5
    JOB_MAX_RETRIES: int = 3

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
""",
    "app/core/db.py": """from collections.abc import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from app.core.config import settings

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    future=True,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True
)

async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False
)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
""",
    "app/core/redis.py": """from redis.asyncio import ConnectionPool, Redis
from app.core.config import settings

pool = ConnectionPool.from_url(
    settings.REDIS_URL,
    max_connections=50,
    decode_responses=True
)

def get_redis() -> Redis:
    return Redis(connection_pool=pool)
""",
    # -------------------------------------------------------------
    # MODELS (SQLAlchemy 2.0 Mapped Syntax)
    # -------------------------------------------------------------
    "app/models/__init__.py": """from app.models.base import Base
from app.models.job import Job, JobAttempt, WorkflowStep
from app.models.outbox import OutboxEvent

__all__ = ["Base", "Job", "JobAttempt", "WorkflowStep", "OutboxEvent"]
""",
    "app/models/base.py": """from datetime import datetime, timezone
from sqlalchemy import DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False
    )
""",
    "app/models/job.py": """import uuid
from datetime import datetime
from typing import Any
from sqlalchemy import DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, TimestampMixin

class Job(Base, TimestampMixin):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    idempotency_key: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False
    )
    workflow_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), default="queued", index=True, nullable=False
    ) # queued, running, succeeded, failed, dead_lettered
    
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error_message: Mapped[str | None] = mapped_column(String, nullable=True)
    
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationships
    attempts: Mapped[list["JobAttempt"]] = relationship(back_populates="job", cascade="all, delete-orphan")
    steps: Mapped[list["WorkflowStep"]] = relationship(back_populates="job", cascade="all, delete-orphan")


class JobAttempt(Base, TimestampMixin):
    __tablename__ = "job_attempts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), index=True, nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    error_log: Mapped[str | None] = mapped_column(String, nullable=True)

    job: Mapped["Job"] = relationship(back_populates="attempts")


class WorkflowStep(Base, TimestampMixin):
    __tablename__ = "workflow_steps"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), index=True, nullable=False)
    step_name: Mapped[str] = mapped_column(String(64), nullable=False) # e.g. parse, embed, index
    step_order: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    output: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    
    job: Mapped["Job"] = relationship(back_populates="steps")

    __table_args__ = (
        Index("ix_workflow_steps_job_order", "job_id", "step_order", unique=True),
    )
""",
    "app/models/outbox.py": """import uuid
from typing import Any
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base, TimestampMixin

class OutboxEvent(Base, TimestampMixin):
    __tablename__ = "outbox_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream_name: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True, nullable=False) # pending, published, failed
""",
    # -------------------------------------------------------------
    # SCHEMAS (Pydantic v2)
    # -------------------------------------------------------------
    "app/schemas/__init__.py": "",
    "app/schemas/job.py": """from datetime import datetime
from typing import Any
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field

class JobCreateRequest(BaseModel):
    workflow_type: str = Field(..., description="Target pipeline: 'document_ingestion'")
    payload: dict[str, Any] = Field(..., description="Input parameters e.g., document URL or raw text")

class WorkflowStepResponse(BaseModel):
    step_name: str
    step_order: int
    status: str
    output: dict[str, Any] | None = None

    model_config = ConfigDict(from_attributes=True)

class JobResponse(BaseModel):
    id: UUID
    idempotency_key: str
    workflow_type: str
    status: str
    payload: dict[str, Any]
    result: dict[str, Any] | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime
    steps: list[WorkflowStepResponse] = []

    model_config = ConfigDict(from_attributes=True)
""",
    # -------------------------------------------------------------
    # API ENDPOINTS
    # -------------------------------------------------------------
    "app/api/__init__.py": "",
    "app/api/v1/__init__.py": "",
    "app/api/v1/router.py": """from fastapi import APIRouter
from app.api.v1.endpoints import jobs

api_router = APIRouter()
api_router.include_router(jobs.router, prefix="/jobs", tags=["Jobs"])
""",
    "app/api/v1/endpoints/__init__.py": "",
    "app/api/v1/endpoints/jobs.py": """from uuid import UUID
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
""",
    # -------------------------------------------------------------
    # ENTRYPOINT
    # -------------------------------------------------------------
    "app/main.py": """from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.router import api_router
from app.core.config import settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup actions (e.g. pool validation)
    yield
    # Graceful shutdown (e.g. close connection pools)

app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(api_router, prefix="/v1")

@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy", "env": settings.APP_ENV}
""",
}


def main():
    print("🚀 Initializing project skeleton...")
    for file_path_str, content in FILES.items():
        file_path = Path(file_path_str)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  ✔ Created {file_path_str}")

    print("\n✅ Skeleton scaffolding completed successfully!")


if __name__ == "__main__":
    main()
