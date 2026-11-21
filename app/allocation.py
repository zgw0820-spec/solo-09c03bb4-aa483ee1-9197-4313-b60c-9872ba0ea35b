"""Pure capacity arithmetic for the borrowable-memory scheduler.

The scheduler models two capacity pools ("shares") on one machine::

    regular share  -> normal transcode / live-replay work
    patch share    -> re-transcode queue for missing replay segments

Both pools draw from one fixed memory budget.  At all times::

    regular.cap_mb + patch.cap_mb <= total_memory_mb

Normally each pool works under its nominal ceiling, so the two caps sum to the
budget.  A loan temporarily moves capacity from one pool to the other in two
separate steps:

1. The lender's cap drops immediately (it stops admitting work above the new
   cap), but the borrowed capacity is *not* handed over yet.
2. Only after the lender's committed occupancy has drained to the reduced cap
   is the amount added to the borrower's cap.  This is the moment the loan
   timer starts.

The reverse happens when the loan expires: the borrower's cap drops to its
nominal ceiling first, and the capacity only returns to the lender after the
borrower's occupancy has drained to that ceiling.

This module has no I/O and no wall-clock dependency; callers pass ``now``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class AllocationError(ValueError):
    """Raised when a capacity mutation would violate the budget."""


@dataclass
class Job:
    """A unit of work admitted into one share."""

    job_id: str
    share: str
    memory_mb: int
    duration_seconds: float
    submitted_at: float
    name: str = ""
    status: str = "pending"  # pending | running | completed | failed
    started_at: float | None = None
    ends_at: float | None = None
    finished_at: float | None = None
    worker_endpoint: str | None = None
    attempts: int = 0
    last_contact_at: float | None = None
    detail: str | None = None


@dataclass
class Share:
    """One capacity pool with its own admission ceiling."""

    name: str
    nominal_mb: int
    cap_mb: int
    reserved_mb: int = 0

    @classmethod
    def create(cls, name: str, nominal_mb: int) -> "Share":
        return cls(name=name, nominal_mb=nominal_mb, cap_mb=nominal_mb, reserved_mb=0)

    @property
    def free_mb(self) -> int:
        return max(self.cap_mb - self.reserved_mb, 0)

    def can_admit(self, memory_mb: int) -> bool:
        return memory_mb > 0 and memory_mb <= self.free_mb

    def admit(self, memory_mb: int) -> None:
        if memory_mb <= 0:
            raise AllocationError("memory_mb must be positive")
        if memory_mb > self.free_mb:
            raise AllocationError(
                f"{self.name}: cannot admit {memory_mb} MB, only {self.free_mb} MB "
                f"free under cap {self.cap_mb} MB"
            )
        self.reserved_mb += memory_mb

    def release(self, memory_mb: int) -> None:
        if memory_mb <= 0:
            raise AllocationError("memory_mb must be positive")
        if memory_mb > self.reserved_mb:
            raise AllocationError(
                f"{self.name}: cannot release {memory_mb} MB, only "
                f"{self.reserved_mb} MB reserved"
            )
        self.reserved_mb -= memory_mb


class LoanStatus(str, Enum):
    """Lifecycle of a capacity loan."""

    RELEASING = "releasing"  # lender cap reduced, waiting for occupancy to drain
    ACTIVE = "active"  # borrower cap increased, loan timer running
    RETURNING = "returning"  # borrower cap reduced, waiting for occupancy to drain
    RETURNED = "returned"  # capacity fully back with the lender


@dataclass
class Loan:
    """A single borrow transaction between two shares."""

    loan_id: str
    amount_mb: int
    duration_seconds: float
    lender: str
    borrower: str
    created_at: float
    release_started_at: float
    note: str = ""
    lender_occupancy_at_start: int = 0
    status: LoanStatus = LoanStatus.RELEASING
    transferred_at: float | None = None
    due_at: float | None = None
    return_started_at: float | None = None
    borrower_occupancy_at_return_start: int | None = None
    returned_at: float | None = None

    @property
    def is_outstanding(self) -> bool:
        return self.status is not LoanStatus.RETURNED


def open_loan(
    loan: Loan,
    shares: dict[str, Share],
    total_memory_mb: int,
) -> None:
    """Apply the lender-side reduction for a fresh loan and assert invariants."""

    lender = shares[loan.lender]
    if loan.amount_mb <= 0:
        raise AllocationError("loan amount must be positive")
    if loan.duration_seconds <= 0:
        raise AllocationError("loan duration must be positive")
    if loan.amount_mb > lender.cap_mb:
        raise AllocationError(
            f"cannot lend {loan.amount_mb} MB from {lender.name}: cap is "
            f"{lender.cap_mb} MB"
        )
    lender.cap_mb -= loan.amount_mb
    loan.lender_occupancy_at_start = lender.reserved_mb
    assert_budget_invariants(shares, total_memory_mb)


def advance_loan(
    loan: Loan,
    shares: dict[str, Share],
    total_memory_mb: int,
    now: float,
) -> bool:
    """Advance one loan as far as the current occupancy allows.

    Returns ``True`` when the loan's state changed.  The transitions are
    sequential on purpose: an expired loan whose borrower is already below its
    nominal cap can move ``active -> returning -> returned`` in one call.
    """

    lender = shares[loan.lender]
    borrower = shares[loan.borrower]
    changed = False

    if loan.status is LoanStatus.RELEASING:
        # Hand over the capacity only once the lender has drained to its new cap.
        if lender.reserved_mb <= lender.cap_mb:
            borrower.cap_mb += loan.amount_mb
            loan.status = LoanStatus.ACTIVE
            loan.transferred_at = now
            loan.due_at = now + loan.duration_seconds
            changed = True

    if (
        loan.status is LoanStatus.ACTIVE
        and loan.due_at is not None
        and now >= loan.due_at
    ):
        # Stop admitting new borrower work above the nominal cap right away.
        borrower.cap_mb -= loan.amount_mb
        loan.status = LoanStatus.RETURNING
        loan.return_started_at = now
        loan.borrower_occupancy_at_return_start = borrower.reserved_mb
        changed = True

    if loan.status is LoanStatus.RETURNING:
        # The capacity goes back only after the borrower's occupancy drains.
        if borrower.reserved_mb <= borrower.cap_mb:
            lender.cap_mb += loan.amount_mb
            loan.status = LoanStatus.RETURNED
            loan.returned_at = now
            changed = True

    assert_budget_invariants(shares, total_memory_mb)
    return changed


def assert_budget_invariants(
    shares: dict[str, Share],
    total_memory_mb: int,
) -> None:
    """Guard the two safety properties the operator relies on."""

    cap_sum = sum(share.cap_mb for share in shares.values())
    if cap_sum > total_memory_mb:
        raise AllocationError(
            f"effective caps total {cap_sum} MB, exceeds budget {total_memory_mb} MB"
        )
    for share in shares.values():
        if share.cap_mb < 0:
            raise AllocationError(f"{share.name}: negative cap {share.cap_mb} MB")
        if share.reserved_mb < 0:
            raise AllocationError(
                f"{share.name}: negative reservation {share.reserved_mb} MB"
            )
    reserved_sum = sum(share.reserved_mb for share in shares.values())
    if reserved_sum > total_memory_mb:
        raise AllocationError(
            f"reserved memory {reserved_sum} MB exceeds budget {total_memory_mb} MB"
        )
