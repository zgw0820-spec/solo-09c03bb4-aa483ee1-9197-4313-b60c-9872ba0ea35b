"""Integration tests: FastAPI + scheduler + in-process mock workers.

The scheduler talks HTTP to the mock workers.  Rather than binding real ports
we mount the worker FastAPI apps under an ``httpx.MockTransport`` so requests
are dispatched in-process and fully deterministic.  Time is controlled by a
fake clock, so loan expiry and job completion can be tested without sleeping.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.scheduler import ConflictError, Scheduler
from app.store import Store
from worker.main import create_app as create_worker_app

REGULAR = "http://regular.test"
PATCH = "http://patch.test"


class FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class WorkerWorld:
    """Routes scheduler HTTP calls to two in-process worker apps."""

    def __init__(self, clock: FakeClock) -> None:
        self.regular = create_worker_app("regular-worker", clock=clock)
        self.patch = create_worker_app("patch-worker", clock=clock)
        self.requests: list[tuple[str, str]] = []

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    async def _handle(self, request: httpx.Request) -> httpx.Response:
        app = {
            "regular.test": self.regular,
            "patch.test": self.patch,
        }.get(request.url.host)
        if app is None:  # pragma: no cover - guard
            return httpx.Response(404, json={"detail": "unknown host"})
        self.requests.append((request.method, request.url.path))
        body = await request.aread()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://worker"
        ) as client:
            response = await client.request(
                request.method,
                request.url.path,
                content=body or None,
                headers={"content-type": request.headers.get("content-type", "")},
            )
        return httpx.Response(
            response.status_code,
            content=response.content,
            headers={
                "content-type": response.headers.get("content-type", "application/json")
            },
        )


def make_settings() -> Settings:
    return Settings(
        node_id="test-node",
        total_memory_mb=4096,
        regular_share_mb=3072,
        patch_share_mb=1024,
        regular_worker_url=REGULAR,
        patch_worker_url=PATCH,
        tick_interval_seconds=0.01,
        worker_timeout_seconds=2.0,
        probe_timeout_seconds=2.0,
    )


@dataclass
class Harness:
    scheduler: Scheduler
    clock: FakeClock
    workers: WorkerWorld
    store: Store

    def tick(self) -> None:
        asyncio.run(self.scheduler.tick())

    def add_job(
        self, share: str, memory: int, duration: float, name: str = ""
    ):
        return self.scheduler.submit_job(
            share=share, memory_mb=memory, duration_seconds=duration, name=name
        )


@pytest.fixture()
def harness(tmp_path: Path):
    clock = FakeClock()
    workers = WorkerWorld(clock)
    store = Store(tmp_path / "scheduler.db")
    client = httpx.AsyncClient(transport=workers.transport())
    scheduler = Scheduler(make_settings(), store, clock=clock, http_client=client)
    yield Harness(scheduler=scheduler, clock=clock, workers=workers, store=store)
    store.close()


def test_job_admitted_within_cap_and_rejected_beyond(harness: Harness) -> None:
    harness.add_job("regular", 3000, 10)
    with pytest.raises(Exception, match="cannot admit"):
        harness.add_job("regular", 100, 10)

    harness.add_job("patch", 1024, 10)
    with pytest.raises(Exception, match="cannot admit"):
        harness.add_job("patch", 1, 10)


def test_job_runs_to_completion_and_releases_memory(harness: Harness) -> None:
    job = harness.add_job("patch", 512, 30)

    harness.tick()
    assert job.status == "running"
    assert harness.scheduler.shares["patch"].reserved_mb == 512

    harness.clock.advance(31)
    harness.tick()
    assert job.status == "completed"
    assert harness.scheduler.shares["patch"].reserved_mb == 0


def test_loan_lifecycle_with_lender_still_running(harness: Harness) -> None:
    scheduler = harness.scheduler

    # Regular queue is full and the job is long-running.
    regular_job = harness.add_job("regular", 3072, 600)
    harness.tick()
    assert regular_job.status == "running"

    loan = scheduler.request_loan(amount_mb=512, duration_seconds=20, note="incident 42")
    # Lender cap is reduced at once, patch gets nothing yet.
    assert scheduler.shares["regular"].cap_mb == 2560
    assert scheduler.shares["patch"].cap_mb == 1024
    assert loan.status.value == "releasing"

    # New regular work is rejected above the reduced cap.
    with pytest.raises(Exception, match="capacity is on loan"):
        harness.add_job("regular", 1, 10)

    # An in-flight job is not revoked: it keeps running.
    assert regular_job.status == "running"

    # Patch work beyond its nominal cap is still rejected.
    with pytest.raises(Exception, match="cannot admit"):
        harness.add_job("patch", 1100, 10)

    # Simulate the lender job finishing early -> handover and timer start.
    harness.clock.advance(600)
    harness.tick()
    assert regular_job.status == "completed"
    assert loan.status.value == "active"
    assert scheduler.shares["regular"].cap_mb == 2560
    assert scheduler.shares["patch"].cap_mb == 1536

    # Patch can now use the borrowed capacity.
    patch_job = harness.add_job("patch", 1536, 30)
    harness.tick()
    assert patch_job.status == "running"

    # Loan expires while the patch job is still running: cap drops, not returned.
    harness.clock.advance(20)
    harness.tick()
    assert loan.status.value == "returning"
    assert scheduler.shares["patch"].cap_mb == 1024
    assert scheduler.shares["regular"].cap_mb == 2560
    assert patch_job.status == "running"

    # The in-flight patch job completes; then the capacity returns.
    harness.clock.advance(11)
    harness.tick()
    assert patch_job.status == "completed"
    assert loan.status.value == "returned"
    assert scheduler.shares["regular"].cap_mb == 3072
    assert scheduler.shares["patch"].cap_mb == 1024
    assert sum(share.cap_mb for share in scheduler.shares.values()) == 4096


def test_expiry_waits_for_borrower_job_and_keeps_running_it(harness: Harness) -> None:
    """Explicit regression: expiry must not kill in-flight work."""

    scheduler = harness.scheduler
    scheduler.request_loan(amount_mb=1024, duration_seconds=5, note="")
    job = harness.add_job("patch", 2048, 60)
    harness.tick()
    assert job.status == "running"

    harness.clock.advance(6)
    harness.tick()
    loan = scheduler.list_loans()[-1]
    assert loan.status.value == "returning"
    assert job.status == "running"
    assert scheduler.shares["patch"].cap_mb == 1024
    assert scheduler.shares["patch"].reserved_mb == 2048

    harness.clock.advance(55)
    harness.tick()
    assert job.status == "completed"
    assert loan.status.value == "returned"


def test_second_loan_rejected_until_first_returned(harness: Harness) -> None:
    scheduler = harness.scheduler
    loan = scheduler.request_loan(amount_mb=256, duration_seconds=5, note="")
    assert loan.status.value == "active"  # lender idle -> immediate handover

    with pytest.raises(ConflictError):
        scheduler.request_loan(amount_mb=128, duration_seconds=5, note="")

    harness.clock.advance(5)
    harness.tick()
    assert loan.status.value == "returned"

    second = scheduler.request_loan(amount_mb=128, duration_seconds=5, note="")
    assert second.status.value == "active"


def test_loan_amount_must_fit_lender_nominal(harness: Harness) -> None:
    with pytest.raises(Exception, match="nominal"):
        harness.scheduler.request_loan(amount_mb=3073, duration_seconds=5, note="")


def test_persistence_restores_caps_and_jobs(tmp_path: Path) -> None:
    clock = FakeClock()
    workers = WorkerWorld(clock)
    db = tmp_path / "db.sqlite"
    store = Store(db)
    scheduler = Scheduler(
        make_settings(),
        store,
        clock=clock,
        http_client=httpx.AsyncClient(transport=workers.transport()),
    )
    harness = Harness(scheduler, clock, workers, store)
    harness.add_job("regular", 1024, 60)
    scheduler.request_loan(amount_mb=512, duration_seconds=30, note="restart test")
    harness.tick()
    store.close()

    store2 = Store(db)
    revived = Scheduler(
        make_settings(),
        store2,
        clock=clock,
        http_client=httpx.AsyncClient(transport=workers.transport()),
    )
    assert revived.shares["regular"].cap_mb == 2560
    assert revived.shares["patch"].cap_mb == 1536
    assert len(revived.jobs) == 1
    outstanding = [
        loan for loan in revived.list_loans() if loan.status.value != "returned"
    ]
    assert len(outstanding) == 1
    store2.close()


def test_api_smoke(harness: Harness) -> None:
    """End-to-end through the HTTP surface, including Swagger availability."""

    from app import api as api_module

    app = api_module.create_app(settings=make_settings(), scheduler=harness.scheduler)

    with TestClient(app) as client:
        assert client.get("/docs").status_code == 200
        schema = client.get("/openapi.json").json()
        assert {"/jobs", "/loans", "/capacity"} <= set(schema["paths"])

        response = client.post(
            "/jobs",
            json={"share": "regular", "memory_mb": 2000, "duration_seconds": 60},
        )
        assert response.status_code == 201, response.text
        job_id = response.json()["job_id"]

        rejected = client.post(
            "/jobs",
            json={"share": "regular", "memory_mb": 2000, "duration_seconds": 60},
        )
        assert rejected.status_code == 422
        assert "cannot admit" in rejected.json()["detail"]

        capacity = client.get("/capacity").json()
        assert capacity["within_budget"] is True
        regular = next(s for s in capacity["shares"] if s["name"] == "regular")
        assert regular["reserved_mb"] == 2000
        assert regular["free_mb"] == 1072

        loan_response = client.post(
            "/loans", json={"amount_mb": 512, "duration_seconds": 30}
        )
        assert loan_response.status_code == 201, loan_response.text
        loan_id = loan_response.json()["loan_id"]

        second = client.post("/loans", json={"amount_mb": 64, "duration_seconds": 30})
        assert second.status_code == 409

        progress = client.get(f"/loans/{loan_id}").json()
        # The regular job (2000 MB) is already below the reduced cap (2560 MB),
        # so the capacity is handed over immediately and the timer starts.
        assert progress["status"] == "active"
        assert progress["lender_cap_mb"] == 2560
        assert progress["borrower_cap_mb"] == 1536
        assert progress["remaining_seconds"] == pytest.approx(30.0)

        jobs = client.get("/jobs").json()
        assert [job["job_id"] for job in jobs] == [job_id]

        assert client.get(f"/loans/{loan_id}/progress").status_code == 200


def test_openapi_schema_is_valid(tmp_path: Path) -> None:
    from app.api import create_app

    app = create_app(settings=make_settings(), database_path=tmp_path / "schema.db")
    schema = app.openapi()
    assert schema["info"]["title"]
    for path in ("/capacity", "/jobs", "/loans"):
        assert path in schema["paths"]
