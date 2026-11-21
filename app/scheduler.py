"""The scheduler core: admission, dispatch, loans and the background ticker.

Design notes
------------
* ``Scheduler`` owns all mutable state.  It runs on one asyncio event loop:
  request handlers and the background tick interleave only at ``await`` points.
  Every state mutation happens in a single synchronous block, so there is no
  lock to take and no partially applied transition.
* Jobs are admitted against the *effective* cap of their share.  Once admitted
  they are never revoked: a loan only ever shrinks the ceiling for *new* work.
* The only loan allowed at a time is a single regular -> patch borrow.  A new
  borrow is refused while one is outstanding.
* A loan moves through ``releasing -> active -> returning -> returned``.  The
  timer starts at ``active``, so the operator's requested duration is fully
  usable.
* The background ticker does three things on a fixed cadence: dispatch queued
  jobs to mock workers, poll running jobs, and advance loans.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable

import httpx

from .allocation import (
    AllocationError,
    Job,
    Loan,
    LoanStatus,
    Share,
    advance_loan,
    assert_budget_invariants,
    open_loan,
)
from .config import Settings
from .store import Store
from .timeutil import iso as _iso

logger = logging.getLogger("scheduler")

LENDER = "regular"
BORROWER = "patch"

Clock = Callable[[], float]


class ConflictError(RuntimeError):
    """Request conflicts with current state (e.g. a loan is outstanding)."""


class Scheduler:
    def __init__(
        self,
        settings: Settings,
        store: Store,
        *,
        clock: Clock | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self._clock: Clock = clock or time.time
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.worker_timeout_seconds
        )
        self.shares: dict[str, Share] = {}
        self.jobs: dict[str, Job] = {}
        self.loans: dict[str, Loan] = {}
        self._ticker_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._ticking = False
        self._restore_or_init()

    # ------------------------------------------------------------------ setup
    def _restore_or_init(self) -> None:
        shares = self.store.load_shares()
        if shares:
            expected = {LENDER, BORROWER}
            if set(shares) != expected:
                raise RuntimeError(f"unexpected shares in database: {set(shares)}")
            restored = {name: share.nominal_mb for name, share in shares.items()}
            configured = {
                LENDER: self.settings.regular_share_mb,
                BORROWER: self.settings.patch_share_mb,
            }
            if restored != configured:
                raise RuntimeError(
                    "persisted share sizes "
                    f"{restored} do not match configuration {configured}; "
                    "refusing to start with an inconsistent budget"
                )
            self.shares = shares
            self.jobs = self.store.load_jobs()
            self.loans = self.store.load_loans()
            # Any job still marked running has to be re-polled; the worker is the
            # source of truth and the first tick reconciles it.
            logger.info(
                "restored %d jobs, %d loans", len(self.jobs), len(self.loans)
            )
            assert_budget_invariants(self.shares, self.settings.total_memory_mb)
            return

        regular = Share.create(LENDER, self.settings.regular_share_mb)
        patch = Share.create(BORROWER, self.settings.patch_share_mb)
        self.shares = {regular.name: regular, patch.name: patch}
        self.jobs = {}
        self.loans = {}
        assert_budget_invariants(self.shares, self.settings.total_memory_mb)
        self._persist()

    def _persist(self) -> None:
        self.store.save_snapshot(self.shares, self.jobs, self.loans)

    def _share_capsule(self) -> dict[str, int]:
        return {name: share.cap_mb for name, share in self.shares.items()}

    def _restore_share_capsule(self, caps: dict[str, int]) -> None:
        for name, cap_mb in caps.items():
            self.shares[name].cap_mb = cap_mb

    # -------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        if self._ticker_task is None or self._ticker_task.done():
            self._stopping.clear()
            self._ticker_task = asyncio.create_task(
                self._run_ticker(), name="scheduler-ticker"
            )

    async def stop(self) -> None:
        self._stopping.set()
        task = self._ticker_task
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if self._owns_client:
            await self._client.aclose()

    async def _run_ticker(self) -> None:
        interval = self.settings.tick_interval_seconds
        while not self._stopping.is_set():
            try:
                await self.tick()
            except asyncio.CancelledError:  # pragma: no cover - shutdown
                raise
            except Exception:  # noqa: BLE001 - keep the ticker alive
                logger.exception("scheduler tick failed")
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def tick(self) -> None:
        """One scheduling round: dispatch -> poll -> advance loans."""

        if self._ticking:
            # A slow worker round is already in flight; never overlap dispatches.
            return
        self._ticking = True
        try:
            await self._dispatch_queued()
            await self._poll_running()
            self._advance_loans()
            self._persist()
        finally:
            self._ticking = False

    # ----------------------------------------------------------- public views
    def capacity_snapshot(self) -> dict:
        now = self._clock()
        shares = []
        for name in (LENDER, BORROWER):
            share = self.shares[name]
            running = [
                job
                for job in self.jobs.values()
                if job.share == name and job.status == "running"
            ]
            shares.append(
                {
                    "name": name,
                    "nominal_mb": share.nominal_mb,
                    "cap_mb": share.cap_mb,
                    "reserved_mb": share.reserved_mb,
                    "free_mb": share.free_mb,
                    "running_jobs": len(running),
                    "over_cap_mb": max(share.reserved_mb - share.cap_mb, 0),
                }
            )
        outstanding = self._outstanding_loan()
        return {
            "node_id": self.settings.node_id,
            "total_memory_mb": self.settings.total_memory_mb,
            "effective_cap_sum_mb": sum(s.cap_mb for s in self.shares.values()),
            "within_budget": sum(s.cap_mb for s in self.shares.values())
            <= self.settings.total_memory_mb,
            "shares": shares,
            "outstanding_loan_id": outstanding.loan_id if outstanding else None,
            "updated_at": _iso(now),
        }

    def loan_view(self, loan: Loan) -> dict:
        lender = self.shares[loan.lender]
        borrower = self.shares[loan.borrower]
        now = self._clock()
        if loan.status is LoanStatus.RELEASING:
            progress = (
                "waiting for lender occupancy to drain to the reduced cap "
                f"({lender.reserved_mb}/{lender.cap_mb} MB); timer not started yet"
            )
        elif loan.status is LoanStatus.ACTIVE:
            progress = "capacity handed over; automatic return scheduled"
        elif loan.status is LoanStatus.RETURNING:
            progress = (
                "return triggered; waiting for borrower occupancy to drain to the "
                f"nominal cap ({borrower.reserved_mb}/{borrower.cap_mb} MB)"
            )
        else:
            progress = "capacity fully returned"
        return {
            "loan_id": loan.loan_id,
            "amount_mb": loan.amount_mb,
            "duration_seconds": loan.duration_seconds,
            "lender": loan.lender,
            "borrower": loan.borrower,
            "note": loan.note,
            "status": loan.status.value,
            "created_at": _iso(loan.created_at),
            "release_started_at": _iso(loan.release_started_at),
            "transferred_at": _iso(loan.transferred_at),
            "due_at": _iso(loan.due_at),
            "return_started_at": _iso(loan.return_started_at),
            "returned_at": _iso(loan.returned_at),
            "remaining_seconds": (
                round(loan.due_at - now, 3)
                if loan.status is LoanStatus.ACTIVE and loan.due_at is not None
                else None
            ),
            "lender_cap_mb": lender.cap_mb,
            "borrower_cap_mb": borrower.cap_mb,
            "lender_reserved_mb": lender.reserved_mb,
            "borrower_reserved_mb": borrower.reserved_mb,
            "lender_drain_needed_mb": max(lender.reserved_mb - lender.cap_mb, 0),
            "borrower_drain_needed_mb": max(borrower.reserved_mb - borrower.cap_mb, 0),
            "progress": progress,
        }

    def job_view(self, job: Job) -> dict:
        now = self._clock()
        remaining = None
        if job.status == "running" and job.ends_at is not None:
            remaining = round(max(job.ends_at - now, 0.0), 3)
        return {
            "job_id": job.job_id,
            "share": job.share,
            "name": job.name,
            "memory_mb": job.memory_mb,
            "duration_seconds": job.duration_seconds,
            "status": job.status,
            "submitted_at": _iso(job.submitted_at),
            "started_at": _iso(job.started_at),
            "ends_at": _iso(job.ends_at),
            "finished_at": _iso(job.finished_at),
            "remaining_seconds": remaining,
            "worker": job.worker_endpoint,
            "detail": job.detail,
        }

    # -------------------------------------------------------------- jobs API
    def submit_job(
        self,
        *,
        share: str,
        memory_mb: int,
        duration_seconds: float,
        name: str,
    ) -> Job:
        if share not in self.shares:
            raise AllocationError(f"unknown share {share!r}")
        if memory_mb <= 0:
            raise AllocationError("memory_mb must be positive")
        if duration_seconds <= 0:
            raise AllocationError("duration_seconds must be positive")

        pool = self.shares[share]
        if not pool.can_admit(memory_mb):
            on_loan = (
                " (capacity is on loan)"
                if pool.cap_mb < pool.nominal_mb
                else ""
            )
            raise AllocationError(
                f"{share} queue cannot admit {memory_mb} MB: free {pool.free_mb} MB "
                f"under effective cap {pool.cap_mb} MB{on_loan}"
            )
        now = self._clock()
        job = Job(
            job_id=uuid.uuid4().hex[:16],
            share=share,
            name=name,
            memory_mb=memory_mb,
            duration_seconds=duration_seconds,
            submitted_at=now,
        )
        # Admission and persistence are one unit: the reservation and the job
        # become visible only after the snapshot is safely on disk.  If SQLite
        # cannot write, undo both so the failed request consumes no capacity and
        # the ticker can never dispatch (or persist) a job the client was told
        # did not exist.
        pool.admit(memory_mb)
        self.jobs[job.job_id] = job
        try:
            assert_budget_invariants(self.shares, self.settings.total_memory_mb)
            self._persist()
        except Exception:
            self.jobs.pop(job.job_id, None)
            pool.release(memory_mb)
            raise
        return job

    # -------------------------------------------------------------- loans API
    def request_loan(
        self, *, amount_mb: int, duration_seconds: float, note: str
    ) -> Loan:
        if amount_mb <= 0:
            raise AllocationError("amount_mb must be positive")
        if duration_seconds <= 0:
            raise AllocationError("duration_seconds must be positive")
        outstanding = self._outstanding_loan()
        if outstanding is not None:
            raise ConflictError(
                f"loan {outstanding.loan_id} is still {outstanding.status.value}; "
                "only one loan at a time"
            )
        lender = self.shares[LENDER]
        if amount_mb > lender.nominal_mb:
            raise AllocationError(
                f"cannot borrow {amount_mb} MB: regular share nominal is "
                f"{lender.nominal_mb} MB"
            )
        now = self._clock()
        loan = Loan(
            loan_id=uuid.uuid4().hex[:12],
            amount_mb=amount_mb,
            duration_seconds=duration_seconds,
            note=note,
            lender=LENDER,
            borrower=BORROWER,
            created_at=now,
            release_started_at=now,
        )
        # Request, any immediate handover and persistence are one unit.  If the
        # snapshot cannot be written, restore every cap and drop the loan so a
        # failed request neither moves capacity nor blocks the next admission or
        # loan request.
        capsule = self._share_capsule()
        try:
            open_loan(loan, self.shares, self.settings.total_memory_mb)
            self.loans[loan.loan_id] = loan
            # If the lender is already below the reduced cap we can hand over now.
            self._advance_loans()
            self._persist()
        except Exception:
            self.loans.pop(loan.loan_id, None)
            self._restore_share_capsule(capsule)
            raise
        logger.info(
            "loan %s requested: %d MB for %.1fs", loan.loan_id, amount_mb, duration_seconds
        )
        return loan

    def list_loans(self) -> list[Loan]:
        return sorted(self.loans.values(), key=lambda loan: loan.created_at)

    def get_loan(self, loan_id: str) -> Loan | None:
        return self.loans.get(loan_id)

    def cancel_loan(self, loan_id: str) -> Loan:
        """Abort a loan that has not handed capacity over yet.

        While a loan is ``releasing`` the borrower has received nothing, so
        undoing the lender's cap reduction is safe and does not affect any
        job.  Once the capacity has moved it can only come back through the
        normal drain path.
        """

        loan = self.loans.get(loan_id)
        if loan is None:
            raise KeyError(loan_id)
        if loan.status is LoanStatus.RELEASING:
            self.shares[loan.lender].cap_mb += loan.amount_mb
            loan.status = LoanStatus.RETURNED
            loan.returned_at = self._clock()
            assert_budget_invariants(self.shares, self.settings.total_memory_mb)
            self._persist()
            logger.info("loan %s cancelled before handover", loan.loan_id)
            return loan
        raise ConflictError(
            f"loan {loan.loan_id} is {loan.status.value}; only a loan that is still "
            "releasing can be cancelled.  Let it return normally."
        )

    def _outstanding_loan(self) -> Loan | None:
        for loan in sorted(self.loans.values(), key=lambda item: item.created_at):
            if loan.status is not LoanStatus.RETURNED:
                return loan
        return None

    def _advance_loans(self) -> None:
        now = self._clock()
        for loan in sorted(self.loans.values(), key=lambda item: item.created_at):
            changed = advance_loan(
                loan, self.shares, self.settings.total_memory_mb, now
            )
            if changed:
                logger.info("loan %s -> %s", loan.loan_id, loan.status.value)

    # ----------------------------------------------------------- dispatch/poll
    def _worker_for(self, share: str) -> str:
        return (
            self.settings.regular_worker_url
            if share == LENDER
            else self.settings.patch_worker_url
        )

    async def _dispatch_queued(self) -> None:
        pending = sorted(
            (job for job in self.jobs.values() if job.status == "pending"),
            key=lambda job: job.submitted_at,
        )
        if not pending:
            return
        # Fan out so one unreachable worker cannot delay the others (or the
        # loan advance that runs after dispatch).
        results = await asyncio.gather(
            *(self._dispatch_one(job) for job in pending), return_exceptions=True
        )
        for job, result in zip(pending, results):
            if isinstance(result, Exception):  # defensive; _dispatch_one catches
                job.detail = f"dispatch failed: {result!r}"
                job.attempts += 1

    async def _dispatch_one(self, job: Job) -> None:
        endpoint = self._worker_for(job.share)
        try:
            response = await self._client.post(
                f"{endpoint}/jobs",
                json={
                    "job_id": job.job_id,
                    "memory_mb": job.memory_mb,
                    "duration_seconds": job.duration_seconds,
                    "cap_mb": self.shares[job.share].cap_mb,
                    "subject": job.name,
                },
                timeout=self.settings.worker_timeout_seconds,
            )
        except httpx.HTTPError as exc:
            job.detail = f"dispatch failed: {exc.__class__.__name__}: {exc}"
            job.attempts += 1
            logger.warning("dispatch %s failed: %s", job.job_id, exc)
            return
        if response.status_code >= 400:
            job.detail = (
                f"dispatch rejected: HTTP {response.status_code} {response.text}"
            )
            job.attempts += 1
            logger.warning("dispatch %s rejected: %s", job.job_id, response.text)
            return
        now = self._clock()
        job.status = "running"
        job.started_at = now
        job.ends_at = now + job.duration_seconds
        job.worker_endpoint = endpoint
        job.last_contact_at = now
        job.attempts += 1
        job.detail = None
        logger.info("job %s running on %s", job.job_id, endpoint)

    def _running_jobs(self) -> list[Job]:
        return [job for job in self.jobs.values() if job.status == "running"]

    async def _poll_running(self) -> None:
        running = [job for job in self._running_jobs() if job.worker_endpoint]
        if not running:
            return
        await asyncio.gather(
            *(self._poll_one(job) for job in running), return_exceptions=True
        )

    async def _poll_one(self, job: Job) -> None:
        assert job.worker_endpoint is not None
        try:
            response = await self._client.get(
                f"{job.worker_endpoint}/jobs/{job.job_id}",
                timeout=self.settings.worker_timeout_seconds,
            )
        except httpx.HTTPError as exc:
            job.detail = f"status poll failed: {exc.__class__.__name__}: {exc}"
            return
        if response.status_code == 404:
            # The worker lost the job (e.g. it restarted).  Requeue it.
            self._requeue(job, "worker lost job; requeued")
            return
        if response.status_code >= 400:
            job.detail = f"status poll error: HTTP {response.status_code}"
            return
        payload = response.json()
        state = payload.get("status")
        if state == "completed":
            self._complete_job(job)
        elif state == "failed":
            self._fail_job(job, payload.get("error") or "worker reported failure")
        elif state == "running":
            job.last_contact_at = self._clock()
            if job.ends_at is not None and self._clock() > job.ends_at + 3600:
                # Safety net: a worker that claims to still be running well
                # past its own deadline is treated as hung.
                self._fail_job(job, "worker timeout: job overran its deadline")

    def _complete_job(self, job: Job) -> None:
        if job.status != "running":
            return
        job.status = "completed"
        job.finished_at = self._clock()
        job.detail = None
        self.shares[job.share].release(job.memory_mb)
        logger.info("job %s completed", job.job_id)

    def _fail_job(self, job: Job, reason: str) -> None:
        if job.status != "running":
            return
        job.status = "failed"
        job.finished_at = self._clock()
        job.detail = reason
        self.shares[job.share].release(job.memory_mb)
        logger.warning("job %s failed: %s", job.job_id, reason)

    def _requeue(self, job: Job, reason: str) -> None:
        """Put a running job back in its queue without changing the reservation."""

        job.status = "pending"
        job.started_at = None
        job.ends_at = None
        job.worker_endpoint = None
        job.detail = reason
        logger.warning("job %s %s", job.job_id, reason)

    def reconcile(self) -> None:
        """Best-effort local recovery: reservations mirror queued+running jobs.

        Called on startup.  In normal operation reservation changes happen at
        admission and completion, so this should be a no-op; it exists so a
        snapshot restored from an older schema cannot drift silently.
        """

        for share in self.shares.values():
            active = [
                job
                for job in self.jobs.values()
                if job.share == share.name and job.status in {"pending", "running"}
            ]
            expected = sum(job.memory_mb for job in active)
            if expected != share.reserved_mb:
                logger.warning(
                    "%s: reserving %d MB from jobs but snapshot said %d MB",
                    share.name,
                    expected,
                    share.reserved_mb,
                )
                share.reserved_mb = expected
        assert_budget_invariants(self.shares, self.settings.total_memory_mb)
        self._persist()

    # ---------------------------------------------------------------- helpers
    async def probe_workers(self) -> list[dict]:
        """Best-effort view of both mock workers for the capacity endpoint."""

        results = []
        for share in (LENDER, BORROWER):
            url = self._worker_for(share)
            entry = {
                "share": share,
                "name": share,
                "url": url,
                "reachable": False,
                "cap_mb": None,
                "used_mb": None,
                "running_jobs": None,
                "last_error": None,
            }
            try:
                response = await self._client.get(
                    f"{url}/health", timeout=self.settings.worker_timeout_seconds
                )
                response.raise_for_status()
                payload = response.json()
                entry.update(
                    reachable=True,
                    cap_mb=payload.get("cap_mb"),
                    used_mb=payload.get("used_mb"),
                    running_jobs=payload.get("running_jobs"),
                )
            except Exception as exc:  # noqa: BLE001 - surfaced to the operator
                entry["last_error"] = f"{exc.__class__.__name__}: {exc}"
            results.append(entry)
        return results

