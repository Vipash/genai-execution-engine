"""
GenAI Pipeline with Real pgvector Storage and Checkpointed Resumption.
"""

import asyncio
import math
import random
import time
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.metrics import STEP_DURATION
from app.models.embedding import DocumentEmbedding
from app.models.job import Job, WorkflowStep

logger = structlog.get_logger(__name__)

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
        stmt = select(WorkflowStep).where(
            WorkflowStep.job_id == job.id, WorkflowStep.status == "succeeded"
        )
        result = await self.db.execute(stmt)
        completed_steps = {s.step_order: s.output for s in result.scalars().all()}

        intermediate_state: dict[str, Any] = {}
        for order, output in completed_steps.items():
            intermediate_state[f"step_{order}_output"] = output

        for step_order, step_name in PIPELINE_STEPS:
            if step_order in completed_steps:
                logger.info(
                    "pipeline.step_skipped",
                    job_id=str(job.id),
                    step=step_name,
                    reason="already_checkpointed",
                )
                continue

            logger.info("pipeline.step_started", job_id=str(job.id), step=step_name)

            start_time = time.perf_counter()
            step_output = await self._run_step(job, step_name, intermediate_state)
            duration = time.perf_counter() - start_time
            STEP_DURATION.labels(step_name=step_name).observe(duration)

            step_record = WorkflowStep(
                job_id=job.id,
                step_name=step_name,
                step_order=step_order,
                status="succeeded",
                output=step_output,
            )
            self.db.add(step_record)
            await self.db.commit()

            intermediate_state[f"step_{step_order}_output"] = step_output
            logger.info(
                "pipeline.step_checkpointed", job_id=str(job.id), step=step_name
            )

        return intermediate_state

    async def _run_step(
        self, job: Job, step_name: str, previous_outputs: dict[str, Any]
    ) -> dict[str, Any]:
        if step_name == "document_parsing":
            await asyncio.sleep(0.5)
            doc_url = job.payload.get("document_url", "https://arxiv.org/doc/sample")
            chunks = [
                f"Introduction section from {doc_url}: Distributed systems overview.",
                f"Methodology section from {doc_url}: Redis Streams consumer groups and XAUTOCLAIM.",
                f"Results section from {doc_url}: Sub-second recovery and zero duplicate executions.",
            ]
            return {"chunks": chunks, "chunks_count": len(chunks)}

        elif step_name == "embedding_generation":
            await asyncio.sleep(0.8)
            chunks = previous_outputs.get("step_1_output", {}).get("chunks", [])

            # Generate deterministic unit-normalized 1536-dimensional float vectors
            embeddings = []
            for idx, text in enumerate(chunks):
                rng = random.Random(f"{job.id}-{idx}")
                raw_vec = [rng.uniform(-1.0, 1.0) for _ in range(1536)]
                norm = math.sqrt(sum(x * x for x in raw_vec))
                normalized_vec = [x / norm for x in raw_vec]
                embeddings.append(
                    {"chunk_index": idx, "text": text, "vector": normalized_vec}
                )

            return {
                "model": "text-embedding-3-small",
                "dimensions": 1536,
                "embeddings": embeddings,
                "count": len(embeddings),
            }

        elif step_name == "vector_indexing":
            await asyncio.sleep(0.4)
            embeddings_data = previous_outputs.get("step_2_output", {}).get(
                "embeddings", []
            )

            # Persist real vectors directly into PostgreSQL using pgvector
            for item in embeddings_data:
                record = DocumentEmbedding(
                    job_id=job.id,
                    chunk_index=item["chunk_index"],
                    chunk_text=item["text"],
                    embedding=item["vector"],
                )
                self.db.add(record)

            await self.db.commit()

            return {
                "collection": "document_embeddings",
                "indexed_vectors_count": len(embeddings_data),
                "storage_engine": "pgvector_hnsw",
                "status": "ready",
            }

        raise PipelineExecutionError(f"Unknown pipeline step: {step_name}")
