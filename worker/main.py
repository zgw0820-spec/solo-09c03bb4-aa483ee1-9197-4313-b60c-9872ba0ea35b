"""A tiny mock transcoding worker.

The real transcode service is out of scope; this stands in for an agent that
holds a fixed amount of memory for a requested duration and then reports the
job as completed.  The scheduler is the sole admission authority; the worker
mirrors the memory it is actually holding so an operator can compare both
sides.

Job state is derived from a clock on every read rather than from background
tasks.  That keeps the worker stateless under ASGI transports and makes tests
able to drive it with a fake clock.

Endpoints
---------
GET  /health        -> {name, used_mb, running_jobs, cap_mb}
GET  /jobs          -> in-memory job table (debug aid)
POST /jobs          -> {job_id, memory_mb, duration_seconds, cap_mb, subject}
GET  /jobs/{job_id} -> {job_id, status, error, remaining_seconds}
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


class JobRequest(BaseModel):
    job_id: str
    memory_mb: int = Field(gt=0)
    duration_seconds: float = Field(gt=0)
    cap_mb: int = Field(gt=0, description="Effective cap the scheduler applied.")
    subject: str = ""


@dataclass
class WorkerJob:
    job_id: str
    memory_mb: int
    duration_seconds: float
    started_at: float
    ends_at: float
    status: str = "running"
    error: str | None = None


@dataclass
class WorkerState:
    name: str
    clock: Callable[[], float] = time.time
    jobs: dict[str, WorkerJob] = field(default_factory=dict)
    last_cap_mb: int | None = None

    def settle(self) -> None:
        """Mark jobs whose deadline has passed as completed."""

        now = self.clock()
        for job in self.jobs.values():
            if job.status == "running" and now >= job.ends_at:
                job.status = "completed"

    @property
    def used_mb(self) -> int:
        self.settle()
        return sum(job.memory_mb for job in self.jobs.values() if job.status == "running")

    @property
    def running_jobs(self) -> int:
        self.settle()
        return sum(1 for job in self.jobs.values() if job.status == "running")


def create_app(
    name: str | None = None, clock: Callable[[], float] | None = None
) -> FastAPI:
    worker_name = name or os.environ.get("WORKER_NAME", "worker")
    app = FastAPI(
        title=f"Mock transcode worker ({worker_name})",
        description="Occupies memory for the requested duration, then completes.",
        version="1.0.0",
    )
    app.state.worker = WorkerState(name=worker_name, clock=clock or time.time)

    @app.get("/health", summary="Worker liveness and current memory usage")
    async def health() -> dict:
        state: WorkerState = app.state.worker
        return {
            "name": state.name,
            "used_mb": state.used_mb,
            "running_jobs": state.running_jobs,
            "cap_mb": state.last_cap_mb,
        }

    @app.get("/jobs", summary="Inspect the worker's in-memory job table")
    async def list_jobs() -> dict:
        state: WorkerState = app.state.worker
        state.settle()
        return {
            "jobs": {
                job.job_id: {
                    "status": job.status,
                    "memory_mb": job.memory_mb,
                    "ends_at": job.ends_at,
                    "error": job.error,
                }
                for job in state.jobs.values()
            }
        }

    @app.post("/jobs", status_code=201, summary="Accept a job")
    async def submit(request: JobRequest) -> dict:
        state: WorkerState = app.state.worker
        state.last_cap_mb = request.cap_mb
        existing = state.jobs.get(request.job_id)
        if existing is not None:
            state.settle()
            return {"job_id": existing.job_id, "status": existing.status}
        # The scheduler is the admission authority: it only dispatches work that
        # was admitted against the share's effective cap.  The worker therefore
        # always accepts and simply occupies the memory for the duration.  This
        # matters while a loan is returning: jobs admitted before the ceiling
        # dropped must still be allowed to run and drain.
        now = state.clock()
        state.jobs[request.job_id] = WorkerJob(
            job_id=request.job_id,
            memory_mb=request.memory_mb,
            duration_seconds=request.duration_seconds,
            started_at=now,
            ends_at=now + request.duration_seconds,
        )
        return {"job_id": request.job_id, "status": "running"}

    @app.get("/jobs/{job_id}", summary="Poll a job's status")
    async def get_job(job_id: str) -> dict:
        state: WorkerState = app.state.worker
        state.settle()
        job = state.jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="unknown job")
        remaining = (
            max(job.ends_at - state.clock(), 0) if job.status == "running" else 0
        )
        return {
            "job_id": job.job_id,
            "status": job.status,
            "error": job.error,
            "remaining_seconds": remaining,
        }

    return app


app = create_app()
