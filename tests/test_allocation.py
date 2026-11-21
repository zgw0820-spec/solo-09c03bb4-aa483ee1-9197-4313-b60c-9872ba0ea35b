"""Tests for the pure capacity arithmetic (no I/O, controlled clock)."""

from __future__ import annotations

import pytest

from app.allocation import (
    AllocationError,
    Loan,
    LoanStatus,
    Share,
    advance_loan,
    assert_budget_invariants,
    open_loan,
)

TOTAL = 4096


def make_shares(regular: int = 3072, patch: int = 1024) -> dict[str, Share]:
    return {
        "regular": Share.create("regular", regular),
        "patch": Share.create("patch", patch),
    }


def make_loan(amount: int, duration: float, now: float) -> Loan:
    return Loan(
        loan_id="loan-1",
        amount_mb=amount,
        duration_seconds=duration,
        lender="regular",
        borrower="patch",
        created_at=now,
        release_started_at=now,
    )


def test_initial_caps_sum_to_budget() -> None:
    shares = make_shares()
    assert_budget_invariants(shares, TOTAL)
    assert shares["regular"].cap_mb + shares["patch"].cap_mb == TOTAL


def test_admission_respects_free_memory() -> None:
    shares = make_shares()
    regular = shares["regular"]
    regular.admit(3000)
    with pytest.raises(AllocationError):
        regular.admit(100)
    regular.release(3000)
    regular.admit(3072)


def test_loan_waits_for_lender_to_drain_before_handover() -> None:
    shares = make_shares()
    regular = shares["regular"]
    regular.admit(3072)  # lender is full

    loan = make_loan(amount=512, duration=60, now=0.0)
    open_loan(loan, shares, TOTAL)

    # Lender cap drops at once; borrower gets nothing yet.
    assert regular.cap_mb == 2560
    assert shares["patch"].cap_mb == 1024
    assert loan.status is LoanStatus.RELEASING

    advance_loan(loan, shares, TOTAL, now=0.0)
    assert loan.status is LoanStatus.RELEASING
    assert shares["patch"].cap_mb == 1024

    # Partial drain is not enough: still above the reduced cap.
    regular.release(256)
    advance_loan(loan, shares, TOTAL, now=1.0)
    assert loan.status is LoanStatus.RELEASING
    assert shares["patch"].cap_mb == 1024

    # Draining to exactly the new cap flips the loan to active.
    regular.release(256)
    assert regular.reserved_mb == 2560
    advance_loan(loan, shares, TOTAL, now=2.0)
    assert loan.status is LoanStatus.ACTIVE
    assert loan.transferred_at == 2.0
    assert loan.due_at == 62.0
    assert shares["patch"].cap_mb == 1536
    assert_budget_invariants(shares, TOTAL)


def test_loan_transfers_immediately_when_lender_is_idle() -> None:
    shares = make_shares()
    loan = make_loan(amount=512, duration=30, now=10.0)
    open_loan(loan, shares, TOTAL)
    advance_loan(loan, shares, TOTAL, now=10.0)
    assert loan.status is LoanStatus.ACTIVE
    assert shares["patch"].cap_mb == 1536
    assert shares["regular"].cap_mb == 2560


def test_timer_starts_at_handover_not_at_request() -> None:
    shares = make_shares()
    shares["regular"].admit(3072)
    loan = make_loan(amount=512, duration=10, now=0.0)
    open_loan(loan, shares, TOTAL)

    # 100 seconds pass while the lender drains; the loan has not started.
    shares["regular"].release(512)
    advance_loan(loan, shares, TOTAL, now=100.0)
    assert loan.status is LoanStatus.ACTIVE
    assert loan.due_at == 110.0


def test_expiry_reduces_borrower_cap_then_waits_for_drain() -> None:
    shares = make_shares()
    loan = make_loan(amount=512, duration=10, now=0.0)
    open_loan(loan, shares, TOTAL)
    advance_loan(loan, shares, TOTAL, now=0.0)
    assert loan.status is LoanStatus.ACTIVE

    # The borrower uses all of its borrowed capacity.
    shares["patch"].admit(1536)
    assert shares["patch"].free_mb == 0

    # Not yet due.
    advance_loan(loan, shares, TOTAL, now=9.0)
    assert loan.status is LoanStatus.ACTIVE

    # Due: ceiling drops immediately, capacity not yet returned.
    advance_loan(loan, shares, TOTAL, now=10.0)
    assert loan.status is LoanStatus.RETURNING
    assert shares["patch"].cap_mb == 1024
    assert shares["regular"].cap_mb == 2560  # still not returned
    assert shares["patch"].reserved_mb == 1536
    assert_budget_invariants(shares, TOTAL)

    # Partially drained.
    shares["patch"].release(256)
    advance_loan(loan, shares, TOTAL, now=11.0)
    assert loan.status is LoanStatus.RETURNING
    assert shares["regular"].cap_mb == 2560

    # Drained to nominal cap -> return.
    shares["patch"].release(256)
    advance_loan(loan, shares, TOTAL, now=12.0)
    assert loan.status is LoanStatus.RETURNED
    assert shares["regular"].cap_mb == 3072
    assert shares["patch"].cap_mb == 1024
    assert_budget_invariants(shares, TOTAL)


def test_expired_loan_can_finish_in_one_step_when_idle() -> None:
    shares = make_shares()
    loan = make_loan(amount=512, duration=10, now=0.0)
    open_loan(loan, shares, TOTAL)
    advance_loan(loan, shares, TOTAL, now=0.0)
    advance_loan(loan, shares, TOTAL, now=10.0)
    # No borrower occupancy, so returning completes in the same call.
    assert loan.status is LoanStatus.RETURNED
    assert shares["regular"].cap_mb == 3072
    assert shares["patch"].cap_mb == 1024


def test_cannot_lend_more_than_lender_cap() -> None:
    shares = make_shares()
    loan = make_loan(amount=4096, duration=10, now=0.0)
    with pytest.raises(AllocationError):
        open_loan(loan, shares, TOTAL)
    # Nothing was mutated by the rejected loan.
    assert shares["regular"].cap_mb == 3072
    assert shares["patch"].cap_mb == 1024


def test_borrower_can_only_admit_up_to_effective_cap() -> None:
    shares = make_shares()
    loan = make_loan(amount=512, duration=10, now=0.0)
    open_loan(loan, shares, TOTAL)
    advance_loan(loan, shares, TOTAL, now=0.0)

    shares["patch"].admit(1024)
    with pytest.raises(AllocationError):
        shares["patch"].admit(513)
    shares["patch"].admit(512)  # exactly the borrowed amount
    assert shares["patch"].free_mb == 0


def test_cap_sum_never_exceeds_budget_across_transitions() -> None:
    shares = make_shares()
    loan = make_loan(amount=700, duration=5, now=0.0)
    open_loan(loan, shares, TOTAL)
    assert_budget_invariants(shares, TOTAL)
    shares["regular"].admit(2000)
    advance_loan(loan, shares, TOTAL, now=0.0)
    shares["regular"].release(2000)
    advance_loan(loan, shares, TOTAL, now=1.0)
    assert_budget_invariants(shares, TOTAL)
    shares["patch"].admit(1724)
    advance_loan(loan, shares, TOTAL, now=6.0)
    assert_budget_invariants(shares, TOTAL)
    shares["patch"].release(1724)
    advance_loan(loan, shares, TOTAL, now=7.0)
    assert_budget_invariants(shares, TOTAL)
    assert loan.status is LoanStatus.RETURNED
