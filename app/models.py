"""Pydantic schemas exposed through the Swagger UI."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class JobCreate(BaseModel):
    """A transcode task.  The sample worker just occupies memory for a while."""

    share: Literal["regular", "patch"] = Field(
        description=(
            "Queue that should take the job.  `patch` is the re-transcode queue "
            "for missing replay segments."
        )
    )
    memory_mb: int = Field(
        gt=0,
        le=1_048_576,
        description="Memory the task occupies for its whole run.",
        examples=[256],
    )
    duration_seconds: float = Field(
        gt=0,
        le=86_400,
        description="How long the task holds the memory.",
        examples=[30],
    )
    name: str = Field(
        default="",
        max_length=120,
        description="Optional human-readable label, e.g. a replay id.",
        examples=["replay-2026-11-21-1930"],
    )


class JobView(BaseModel):
    job_id: str
    share: str
    name: str
    memory_mb: int
    duration_seconds: float
    status: Literal["pending", "running", "completed", "failed"]
    submitted_at: str
    started_at: str | None
    ends_at: str | None
    finished_at: str | None
    remaining_seconds: float | None
    worker: str | None
    detail: str | None


class ShareView(BaseModel):
    name: str
    nominal_mb: int
    cap_mb: int = Field(description="Effective ceiling currently enforced.")
    reserved_mb: int = Field(description="Committed by accepted jobs (queued+running).")
    free_mb: int
    running_jobs: int
    over_cap_mb: int = Field(
        default=0,
        description=(
            "How far committed memory currently sits above the effective cap. "
            "Non-zero only while an outstanding loan waits for work to drain."
        ),
    )


class WorkerView(BaseModel):
    share: str
    name: str
    url: str
    reachable: bool
    cap_mb: int | None
    used_mb: int | None
    running_jobs: int | None
    last_error: str | None


class CapacityView(BaseModel):
    node_id: str
    total_memory_mb: int
    effective_cap_sum_mb: int
    within_budget: bool
    shares: list[ShareView]
    workers: list[WorkerView]
    outstanding_loan_id: str | None
    updated_at: str


class LoanCreate(BaseModel):
    """Borrow memory from the regular queue for the re-transcode queue."""

    amount_mb: int = Field(
        gt=0,
        description="Memory to move from the regular share to the patch share.",
        examples=[512],
    )
    duration_seconds: float = Field(
        gt=0,
        le=86_400,
        description=(
            "How long the borrowed capacity may be used.  The timer starts when "
            "the capacity is actually handed to the patch share, not when the "
            "request is accepted."
        ),
        examples=[60],
    )
    note: str = Field(default="", max_length=200, description="Operator note.")


class LoanView(BaseModel):
    loan_id: str
    amount_mb: int
    duration_seconds: float
    lender: str
    borrower: str
    note: str
    status: Literal["releasing", "active", "returning", "returned"]
    created_at: str
    release_started_at: str
    transferred_at: str | None
    due_at: str | None
    return_started_at: str | None
    returned_at: str | None
    remaining_seconds: float | None = Field(
        default=None,
        description="Seconds until the automatic return starts; only while active.",
    )
    lender_cap_mb: int
    borrower_cap_mb: int
    lender_reserved_mb: int
    borrower_reserved_mb: int
    lender_drain_needed_mb: int = Field(
        default=0,
        description="Lender memory that must finish before the capacity is handed over.",
    )
    borrower_drain_needed_mb: int = Field(
        default=0,
        description="Borrower memory that must finish before the capacity goes back.",
    )
    progress: str


class ErrorDetail(BaseModel):
    detail: str
