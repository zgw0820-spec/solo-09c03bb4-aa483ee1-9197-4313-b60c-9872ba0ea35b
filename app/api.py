"""FastAPI surface for the segment-patch transcode scheduler.

Swagger UI is served at ``/docs``; every operation below is meant to be driven
from there by the on-call engineer.
"""

from __future__ import annotations

import contextlib
import logging
import os
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from .allocation import AllocationError
from .config import Settings
from .models import (
    CapacityView,
    JobCreate,
    JobView,
    LoanCreate,
    LoanView,
    ShareView,
    WorkerView,
)
from .scheduler import ConflictError, Scheduler
from .store import Store
from .timeutil import iso as _iso

logger = logging.getLogger("scheduler.api")


def create_app(
    *,
    settings: Settings | None = None,
    database_path: Path | None = None,
    scheduler: Scheduler | None = None,
) -> FastAPI:
    """Build the FastAPI application.

    ``scheduler`` is injectable so tests can supply a fake clock and a mock
    transport; the production entrypoint uses the defaults.
    """

    settings = settings or Settings.from_env()
    if scheduler is None:
        path = database_path or Path(
            os.environ.get(
                "DATABASE_PATH",
                str(Path(__file__).resolve().parent.parent / "data" / "scheduler.db"),
            )
        )
        store = Store(path)
        scheduler = Scheduler(settings, store)
    else:
        store = scheduler.store
    # Reservations are derived state; rebuild them from the job table on boot.
    scheduler.reconcile()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        await scheduler.start()
        try:
            yield
        finally:
            await scheduler.stop()
            store.close()

    app = FastAPI(
        title="Replay patch-transcode memory scheduler",
        description=(
            "When replay assets are missing segments, the patch re-transcode queue "
            "temporarily borrows memory from the machine's regular transcode queue. "
            "Both queues share one fixed memory budget; effective ceilings always "
            "sum to at most that budget."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.scheduler = scheduler
    app.state.settings = settings
    app.state.store = store

    # ---------------------------------------------------------------- errors
    @app.exception_handler(AllocationError)
    async def _allocation_error(request: Request, exc: AllocationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(ConflictError)
    async def _conflict_error(request: Request, exc: ConflictError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    # ---------------------------------------------------------------- health
    @app.get("/health", tags=["ops"], summary="Scheduler liveness")
    async def health() -> dict:
        return {"status": "ok", "node_id": settings.node_id, "time": _iso(time.time())}

    # ------------------------------------------------------------- capacity
    @app.get(
        "/capacity",
        response_model=CapacityView,
        tags=["capacity"],
        summary="Effective caps, occupancy and worker reachability",
    )
    async def capacity() -> CapacityView:
        snapshot = scheduler.capacity_snapshot()
        workers = await scheduler.probe_workers()
        snapshot["workers"] = [WorkerView(**worker) for worker in workers]
        return CapacityView(**snapshot)

    @app.get(
        "/capacity/shares",
        response_model=list[ShareView],
        tags=["capacity"],
        summary="Just the two share ceilings",
    )
    async def shares() -> list[ShareView]:
        return [ShareView(**share) for share in scheduler.capacity_snapshot()["shares"]]

    # ------------------------------------------------------------------ jobs
    @app.post(
        "/jobs",
        response_model=JobView,
        status_code=201,
        tags=["jobs"],
        summary="Submit a transcode task to one queue",
        responses={
            422: {
                "description": (
                    "Rejected: the request is larger than the queue's current free "
                    "memory (e.g. while its ceiling is reduced by a loan)."
                )
            }
        },
    )
    async def submit_job(payload: JobCreate) -> JobView:
        job = scheduler.submit_job(
            share=payload.share,
            memory_mb=payload.memory_mb,
            duration_seconds=payload.duration_seconds,
            name=payload.name,
        )
        return JobView(**scheduler.job_view(job))

    @app.get(
        "/jobs",
        response_model=list[JobView],
        tags=["jobs"],
        summary="List tasks, newest last",
    )
    async def list_jobs(
        share: str | None = Query(default=None, pattern="^(regular|patch)$"),
        status_filter: str | None = Query(
            default=None, alias="status", pattern="^(pending|running|completed|failed)$"
        ),
    ) -> list[JobView]:
        jobs = sorted(scheduler.jobs.values(), key=lambda job: job.submitted_at)
        if share:
            jobs = [job for job in jobs if job.share == share]
        if status_filter:
            jobs = [job for job in jobs if job.status == status_filter]
        return [JobView(**scheduler.job_view(job)) for job in jobs]

    @app.get(
        "/jobs/{job_id}",
        response_model=JobView,
        tags=["jobs"],
        summary="Inspect one task",
    )
    async def get_job(job_id: str) -> JobView:
        job = scheduler.jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="unknown job")
        return JobView(**scheduler.job_view(job))

    # ----------------------------------------------------------------- loans
    @app.post(
        "/loans",
        response_model=LoanView,
        status_code=201,
        tags=["loans"],
        summary="Borrow regular-queue memory for the patch queue",
        responses={
            409: {"description": "A previous loan has not been fully returned yet."},
            422: {"description": "Invalid amount/duration, or exceeds the share."},
        },
    )
    async def create_loan(payload: LoanCreate) -> LoanView:
        loan = scheduler.request_loan(
            amount_mb=payload.amount_mb,
            duration_seconds=payload.duration_seconds,
            note=payload.note,
        )
        return LoanView(**scheduler.loan_view(loan))

    @app.get(
        "/loans",
        response_model=list[LoanView],
        tags=["loans"],
        summary="All loans, oldest first",
    )
    async def list_loans() -> list[LoanView]:
        return [LoanView(**scheduler.loan_view(loan)) for loan in scheduler.list_loans()]

    @app.get(
        "/loans/{loan_id}",
        response_model=LoanView,
        tags=["loans"],
        summary="Loan status: caps, drain needed, remaining time",
    )
    async def get_loan(loan_id: str) -> LoanView:
        loan = scheduler.get_loan(loan_id)
        if loan is None:
            raise HTTPException(status_code=404, detail="unknown loan")
        return LoanView(**scheduler.loan_view(loan))

    @app.get(
        "/loans/{loan_id}/progress",
        response_model=LoanView,
        tags=["loans"],
        summary="Alias of GET /loans/{loan_id} focused on return progress",
    )
    async def loan_progress(loan_id: str) -> LoanView:
        return await get_loan(loan_id)

    @app.delete(
        "/loans/{loan_id}",
        response_model=LoanView,
        tags=["loans"],
        summary="Cancel a loan that has not handed capacity over yet",
        responses={
            404: {"description": "Unknown loan."},
            409: {
                "description": (
                    "The capacity has already moved to the borrower; it can only "
                    "return through the normal drain path."
                )
            },
        },
    )
    async def cancel_loan(loan_id: str) -> LoanView:
        try:
            loan = scheduler.cancel_loan(loan_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="unknown loan") from None
        return LoanView(**scheduler.loan_view(loan))

    return app
