"""
GenAI Pipeline Executor with Checkpointed Step Resumption.
"""
import asyncio
import time
from datetime import datetime, timezone
from typing import Any
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.job import Job, WorkflowStep
from app.core.metrics import STEP_DURATION

logger = structlog.get_logger(__name__)

# Registered steps in order of execution
PIPELINE_STEPS = [
    (1, "document_parsing"),
    (2, "embedding_generation"),
    (3, "vector_indexing"),
]

class PipelineExecutionError(Exception):
    pass

class WorkflowPipeline:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def execute(self, job: Job) -> dict[str, Any]:
        """
        Executes pipeline steps in strict order.
        Skips steps that have already succeeded in prior attempts.
        """
        # 1. Fetch completed steps for this job
        stmt = select(WorkflowStep).where(
            WorkflowStep.job_id == job.id,
            WorkflowStep.status == "succeeded"
        )
        result = await self.db.execute(stmt)
        completed_steps = {s.step_order: s.output for s in result.scalars().all()}
        
        intermediate_state: dict[str, Any] = {}
        for order, output in completed_steps.items():
            intermediate_state[f"step_{order}_output"] = output

        # 2. Iterate through pipeline steps
        for step_order, step_name in PIPELINE_STEPS:
            if step_order in completed_steps:
                logger.info(
                    "pipeline.step_skipped",
                    job_id=str(job.id),
                    step=step_name,
                    reason="already_checkpointed"
                )
                continue

            logger.info("pipeline.step_started", job_id=str(job.id), step=step_name)
            
            # Instrument step execution latency
            start_time = time.perf_counter()
            step_output = await self._run_step(step_name, job.payload, intermediate_state)
            duration = time.perf_counter() - start_time
            STEP_DURATION.labels(step_name=step_name).observe(duration)
            
            # Checkpoint the successful step in PostgreSQL
            step_record = WorkflowStep(
                job_id=job.id,
                step_name=step_name,
                step_order=step_order,
                status="succeeded",
                output=step_output
            )
            self.db.add(step_record)
            await self.db.commit()
            
            intermediate_state[f"step_{step_order}_output"] = step_output
            logger.info("pipeline.step_checkpointed", job_id=str(job.id), step=step_name)

        return intermediate_state

    async def _run_step(
        self,
        step_name: str,
        input_payload: dict[str, Any],
        previous_outputs: dict[str, Any]
    ) -> dict[str, Any]:
        """Simulates asynchronous GenAI processing latency and logic."""
        if step_name == "document_parsing":
            await asyncio.sleep(1.5)  # Simulate I/O bound fetch & chunking
            doc_url = input_payload.get("document_url", "unknown")
            return {
                "source": doc_url,
                "chunks_count": 12,
                "parsed_tokens": 3450
            }

        elif step_name == "embedding_generation":
            await asyncio.sleep(2.0)  # Simulate GPU embedding inference
            chunks = previous_outputs.get("step_1_output", {}).get("chunks_count", 0)
            return {
                "model": "text-embedding-3-small",
                "embeddings_generated": chunks,
                "dimensions": 1536
            }

        elif step_name == "vector_indexing":
            await asyncio.sleep(1.0)  # Simulate pgvector insertion
            return {
                "collection": "genai_knowledge_base",
                "indexed_vectors": 12,
                "status": "ready"
            }

        raise PipelineExecutionError(f"Unknown pipeline step: {step_name}")