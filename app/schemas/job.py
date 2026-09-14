from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class JobCreateRequest(BaseModel):
    workflow_type: str = Field(..., description="Target pipeline: 'document_ingestion'")
    payload: dict[str, Any] = Field(
        ..., description="Input parameters e.g., document URL or raw text"
    )


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
