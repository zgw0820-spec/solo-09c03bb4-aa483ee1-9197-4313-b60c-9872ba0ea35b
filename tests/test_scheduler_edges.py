"""Edge-case tests around loan conflicts, cancellation and budget guards."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from app.allocation import AllocationError
from app.config import Settings
from app.scheduler import ConflictError, Scheduler
from app.store import Store

from tests.test_api import REGULAR, PATCH, FakeClock, WorkerWorld, make_settings


def build(tmp_path: Path) -> tuple[Scheduler, FakeClock, Store]:
    clock = FakeClock()
    workers = WorkerWorld(clock)
    store = Store(tmp_path / "db.sqlite")
    scheduler = Scheduler(
        make_settings(),
        store,
        clock=clock,
        http_client=httpx.AsyncClient(transport=workers.transport()),
    )
    return scheduler, clock, store


def test_cannot_cancel_initiated_loan(tmp_path: Path) -> None:
    scheduler, clock, store = build(tmp_path)
    loan = scheduler.request_loan(amount_mb=256, duration_seconds=10, note="")
    assert loan.status.value == "active"
    with pytest.raises(ConflictError):
        scheduler.cancel_loan(loan.loan_id)
    assert scheduler.shares["patch"].cap_mb == 1280
    store.close()


def test_cancel_releasing_loan_restores_lender_cap(tmp_path: Path) -> None:
    scheduler, clock, store = build(tmp_path)
    scheduler.submit_job(share="regular", memory_mb=3072, duration_seconds=60, name="")
    asyncio.run(scheduler.tick())
    loan = scheduler.request_loan(amount_mb=512, duration_seconds=10, note="")
    assert loan.status.value == "releasing"
    assert scheduler.shares["regular"].cap_mb == 2560

    cancelled = scheduler.cancel_loan(loan.loan_id)
    assert cancelled.status.value == "returned"
    assert scheduler.shares["regular"].cap_mb == 3072
    assert scheduler.shares["patch"].cap_mb == 1024
    store.close()


def test_new_loan_allowed_after_cancel(tmp_path: Path) -> None:
    scheduler, clock, store = build(tmp_path)
    scheduler.submit_job(share="regular", memory_mb=3000, duration_seconds=60, name="")
    asyncio.run(scheduler.tick())
    first = scheduler.request_loan(amount_mb=512, duration_seconds=10, note="")
    assert first.status.value == "releasing"
    scheduler.cancel_loan(first.loan_id)
    second = scheduler.request_loan(amount_mb=512, duration_seconds=10, note="")
    assert second.status.value == "releasing"
    store.close()


def test_loan_amount_above_free_memory_still_allowed_when_lender_drains(
    tmp_path: Path,
) -> None:
    """The loan is a promise, not an immediate allocation: it may exceed free."""

    scheduler, clock, store = build(tmp_path)
    scheduler.submit_job(share="regular", memory_mb=3072, duration_seconds=30, name="")
    asyncio.run(scheduler.tick())
    loan = scheduler.request_loan(amount_mb=2048, duration_seconds=5, note="")
    assert loan.status.value == "releasing"
    assert scheduler.shares["regular"].cap_mb == 1024
    store.close()


def test_restart_with_changed_configuration_is_refused(tmp_path: Path) -> None:
    clock = FakeClock()
    workers = WorkerWorld(clock)
    db = tmp_path / "db.sqlite"
    store = Store(db)
    Scheduler(
        make_settings(),
        store,
        clock=clock,
        http_client=httpx.AsyncClient(transport=workers.transport()),
    ).list_loans()
    store.close()

    other = Settings(
        node_id="test-node",
        total_memory_mb=4096,
        regular_share_mb=2048,
        patch_share_mb=2048,
        regular_worker_url=REGULAR,
        patch_worker_url=PATCH,
        tick_interval_seconds=0.01,
        worker_timeout_seconds=2.0,
        probe_timeout_seconds=2.0,
    )
    store2 = Store(db)
    with pytest.raises(RuntimeError, match="do not match configuration"):
        Scheduler(other, store2)
    store2.close()


def test_submitting_unknown_share_rejected(tmp_path: Path) -> None:
    scheduler, _, store = build(tmp_path)
    with pytest.raises(AllocationError, match="unknown share"):
        scheduler.submit_job(share="bogus", memory_mb=1, duration_seconds=1, name="")
    store.close()


def test_lender_keeps_accepting_work_under_reduced_cap_while_releasing(
    tmp_path: Path,
) -> None:
    scheduler, clock, store = build(tmp_path)
    scheduler.submit_job(share="regular", memory_mb=3000, duration_seconds=60, name="")
    asyncio.run(scheduler.tick())
    scheduler.request_loan(amount_mb=512, duration_seconds=10, note="")
    # Reduced cap is 2560 and 3000 MB is committed -> free is 0 for now.
    with pytest.raises(AllocationError):
        scheduler.submit_job(share="regular", memory_mb=1, duration_seconds=1, name="")
    # A job that already finished frees room: the lender can use its reduced cap.
    clock.advance(61)
    asyncio.run(scheduler.tick())
    scheduler.submit_job(share="regular", memory_mb=2560, duration_seconds=5, name="")
    with pytest.raises(AllocationError):
        scheduler.submit_job(share="regular", memory_mb=1, duration_seconds=1, name="")
    store.close()


def test_borrower_under_reduced_cap_while_returning(tmp_path: Path) -> None:
    scheduler, clock, store = build(tmp_path)
    scheduler.request_loan(amount_mb=512, duration_seconds=5, note="")
    assert scheduler.shares["patch"].cap_mb == 1536
    job = scheduler.submit_job(
        share="patch", memory_mb=1200, duration_seconds=60, name="patch-1"
    )
    asyncio.run(scheduler.tick())
    assert job.status == "running"

    clock.advance(5)
    asyncio.run(scheduler.tick())
    loan = scheduler.list_loans()[-1]
    assert loan.status.value == "returning"
    # 1200 MB is still committed and the ceiling is back to 1024: free is 0.
    assert scheduler.shares["patch"].free_mb == 0
    with pytest.raises(AllocationError):
        scheduler.submit_job(share="patch", memory_mb=1, duration_seconds=1, name="")
    store.close()
