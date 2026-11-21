"""Write-failure behaviour at the two admission entry points.

The scheduler keeps its state in memory and flushes a full snapshot.  If that
flush fails, the in-memory mutation must be undone so that:

* the failed job consumes no capacity and is never dispatched;
* the failed loan moves no cap, blocks no admission and is never persisted by a
  later tick;
* already admitted work keeps running and releases its memory normally;
* once SQLite is writable again the same request can be resubmitted.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app import api as api_module
from app.scheduler import ConflictError, Scheduler
from app.store import StorageError, Store

from tests.test_api import FakeClock, WorkerWorld, make_settings


class FlakyStore(Store):
    """Real SQLite store that can be told to fail snapshot writes."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.fail_writes = False
        self.write_attempts = 0

    def save_snapshot(self, shares, jobs, loans) -> None:  # type: ignore[no-untyped-def]
        self.write_attempts += 1
        if self.fail_writes:
            raise StorageError("could not save scheduler state: disk I/O error")
        super().save_snapshot(shares, jobs, loans)


def build(tmp_path: Path) -> tuple[Scheduler, FlakyStore, FakeClock, WorkerWorld]:
    clock = FakeClock()
    workers = WorkerWorld(clock)
    store = FlakyStore(tmp_path / "db.sqlite")
    scheduler = Scheduler(
        make_settings(),
        store,
        clock=clock,
        http_client=httpx.AsyncClient(transport=workers.transport()),
    )
    return scheduler, store, clock, workers


def test_failed_job_write_rolls_back_job_and_reservation(tmp_path: Path) -> None:
    scheduler, store, clock, workers = build(tmp_path)
    store.fail_writes = True

    with pytest.raises(StorageError):
        scheduler.submit_job(
            share="regular", memory_mb=3000, duration_seconds=60, name="lost"
        )

    # Nothing consumed, nothing queued, nobody dispatched.  A later tick (with
    # storage healthy again) must not resurrect or dispatch the failed submit.
    assert scheduler.jobs == {}
    assert scheduler.shares["regular"].reserved_mb == 0
    assert scheduler.shares["regular"].free_mb == 3072
    store.fail_writes = False
    asyncio.run(scheduler.tick())
    assert workers.requests == []
    assert scheduler.store.load_jobs() == {}

    # The whole capacity is still available to a new request.
    job = scheduler.submit_job(
        share="regular", memory_mb=3072, duration_seconds=60, name="kept"
    )
    assert scheduler.jobs == {job.job_id: job}
    store.close()


def test_failed_loan_write_restores_caps_and_does_not_block(tmp_path: Path) -> None:
    scheduler, store, clock, workers = build(tmp_path)
    # Lender idle, so a successful loan would immediately go active.
    store.fail_writes = True

    with pytest.raises(StorageError):
        scheduler.request_loan(amount_mb=512, duration_seconds=60, note="failed")

    assert scheduler.loans == {}
    assert scheduler.shares["regular"].cap_mb == 3072
    assert scheduler.shares["patch"].cap_mb == 1024
    assert scheduler._outstanding_loan() is None

    # A ghost reduced cap or reservation would reject a full-size admission.
    store.fail_writes = False
    scheduler.submit_job(
        share="regular", memory_mb=3072, duration_seconds=60, name="full-regular"
    )
    scheduler.submit_job(
        share="patch", memory_mb=1024, duration_seconds=60, name="full-patch"
    )

    # A later tick must not persist the ghost loan, and the next matching
    # request is not refused by it.
    asyncio.run(scheduler.tick())
    assert scheduler.loans == {}
    assert scheduler.store.load_loans() == {}

    store.fail_writes = True
    with pytest.raises(StorageError):
        scheduler.request_loan(amount_mb=256, duration_seconds=60, note="again")
    assert scheduler._outstanding_loan() is None

    store.fail_writes = False
    ok = scheduler.request_loan(amount_mb=256, duration_seconds=60, note="works")
    assert scheduler._outstanding_loan() is ok
    store.close()


def test_failed_loan_write_with_busy_lender_restores_reduced_cap(tmp_path: Path) -> None:
    scheduler, store, clock, workers = build(tmp_path)
    scheduler.submit_job(
        share="regular", memory_mb=3072, duration_seconds=60, name="fill"
    )
    asyncio.run(scheduler.tick())

    store.fail_writes = True
    with pytest.raises(StorageError):
        scheduler.request_loan(amount_mb=512, duration_seconds=60, note="")
    assert scheduler.shares["regular"].cap_mb == 3072
    assert scheduler.shares["patch"].cap_mb == 1024
    assert scheduler.loans == {}
    store.close()


def test_failed_write_does_not_disturb_running_jobs(tmp_path: Path) -> None:
    scheduler, store, clock, workers = build(tmp_path)
    job = scheduler.submit_job(
        share="patch", memory_mb=512, duration_seconds=30, name="in-flight"
    )
    asyncio.run(scheduler.tick())
    assert job.status == "running"

    store.fail_writes = True
    with pytest.raises(StorageError):
        scheduler.submit_job(share="patch", memory_mb=256, duration_seconds=30, name="")
    with pytest.raises(StorageError):
        scheduler.request_loan(amount_mb=128, duration_seconds=30, note="")
    assert job.status == "running"
    assert scheduler.shares["patch"].reserved_mb == 512

    # Even while the store is unwritable the tick keeps advancing and the
    # accepted job completes and releases its reservation.
    clock.advance(31)
    with pytest.raises(StorageError):
        asyncio.run(scheduler.tick())
    assert job.status == "completed"
    assert scheduler.shares["patch"].reserved_mb == 0
    store.close()


def test_real_sqlite_write_failure_then_recovery(tmp_path: Path) -> None:
    """Drive a genuine SQLite failure (read-only database), not just a stub."""

    clock = FakeClock()
    workers = WorkerWorld(clock)
    store = Store(tmp_path / "real.db")
    scheduler = Scheduler(
        make_settings(),
        store,
        clock=clock,
        http_client=httpx.AsyncClient(transport=workers.transport()),
    )

    store._connection.execute("PRAGMA query_only=ON")
    with pytest.raises(StorageError):
        scheduler.submit_job(share="regular", memory_mb=1000, duration_seconds=60, name="")
    with pytest.raises(StorageError):
        scheduler.request_loan(amount_mb=512, duration_seconds=60, note="")

    assert scheduler.jobs == {}
    assert scheduler.loans == {}
    assert scheduler.shares["regular"].cap_mb == 3072
    assert scheduler.shares["regular"].reserved_mb == 0

    # Storage recovers; the identical requests now succeed.
    store._connection.execute("PRAGMA query_only=OFF")
    job = scheduler.submit_job(share="regular", memory_mb=1000, duration_seconds=60, name="")
    loan = scheduler.request_loan(amount_mb=512, duration_seconds=60, note="")
    assert scheduler.jobs == {job.job_id: job}
    assert scheduler.loans == {loan.loan_id: loan}

    # Restart: only the successful state was persisted.
    store.close()
    revived = Scheduler(
        make_settings(),
        Store(tmp_path / "real.db"),
        clock=clock,
        http_client=httpx.AsyncClient(transport=workers.transport()),
    )
    assert set(revived.jobs) == {job.job_id}
    assert set(revived.loans) == {loan.loan_id}
    revived.store.close()


def test_api_reports_retryable_503_and_keeps_capacity(tmp_path: Path) -> None:
    scheduler, store, clock, workers = build(tmp_path)
    app = api_module.create_app(settings=make_settings(), scheduler=scheduler)
    store.fail_writes = True

    with TestClient(app) as client:
        response = client.post(
            "/jobs",
            json={"share": "regular", "memory_mb": 2000, "duration_seconds": 60},
        )
        assert response.status_code == 503
        assert "Retry" in response.json()["detail"]

        loan_response = client.post(
            "/loans", json={"amount_mb": 512, "duration_seconds": 60}
        )
        assert loan_response.status_code == 503
        assert "Retry" in loan_response.json()["detail"]

        capacity = client.get("/capacity").json()
        regular = next(s for s in capacity["shares"] if s["name"] == "regular")
        assert regular["reserved_mb"] == 0
        assert regular["cap_mb"] == 3072
        assert capacity["outstanding_loan_id"] is None

        store.fail_writes = False
        retried = client.post(
            "/jobs",
            json={"share": "regular", "memory_mb": 2000, "duration_seconds": 60},
        )
        assert retried.status_code == 201, retried.text
        retried_loan = client.post(
            "/loans", json={"amount_mb": 512, "duration_seconds": 60}
        )
        assert retried_loan.status_code == 201, retried_loan.text
    store.close()


def test_not_found_and_conflict_semantics_unchanged(tmp_path: Path) -> None:
    """Guard rail: the rollback did not turn real 409s into 503s."""

    scheduler, store, clock, workers = build(tmp_path)
    loan = scheduler.request_loan(amount_mb=256, duration_seconds=60, note="")
    with pytest.raises(ConflictError):
        scheduler.request_loan(amount_mb=128, duration_seconds=60, note="")
    assert scheduler._outstanding_loan() is loan
    store.close()
