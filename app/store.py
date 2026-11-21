"""SQLite persistence for scheduler state.

One scheduler process owns the database.  Every mutation is applied in memory
first and then flushed as a full snapshot inside a single transaction, which
keeps the caps, loans and job reservations mutually consistent even if the
process dies mid-tick.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from pathlib import Path

from .allocation import Job, Loan, LoanStatus, Share

logger = logging.getLogger("scheduler.store")


class StorageError(RuntimeError):
    """Raised when SQLite cannot persist a scheduler snapshot.

    The snapshot write is transactional, so when this is raised the database
    still holds the previous consistent state.  Callers that already mutated
    in-memory state (the request entry points) must undo those mutations before
    re-raising; the API surfaces this as a retryable 503.
    """


SCHEMA = """
CREATE TABLE IF NOT EXISTS shares (
    name        TEXT PRIMARY KEY,
    nominal_mb  INTEGER NOT NULL,
    cap_mb      INTEGER NOT NULL,
    reserved_mb INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id           TEXT PRIMARY KEY,
    share            TEXT NOT NULL,
    name             TEXT NOT NULL,
    memory_mb        INTEGER NOT NULL,
    duration_seconds REAL NOT NULL,
    status           TEXT NOT NULL,
    submitted_at     REAL NOT NULL,
    started_at       REAL,
    ends_at          REAL,
    finished_at      REAL,
    worker           TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_contact_at  REAL,
    detail           TEXT
);

CREATE TABLE IF NOT EXISTS loans (
    loan_id             TEXT PRIMARY KEY,
    amount_mb           INTEGER NOT NULL,
    duration_seconds    REAL NOT NULL,
    note                TEXT NOT NULL DEFAULT '',
    lender              TEXT NOT NULL,
    borrower            TEXT NOT NULL,
    created_at          REAL NOT NULL,
    release_started_at  REAL NOT NULL,
    lender_occupancy_at_start INTEGER NOT NULL DEFAULT 0,
    status              TEXT NOT NULL,
    transferred_at      REAL,
    due_at              REAL,
    return_started_at   REAL,
    borrower_occupancy_at_return_start INTEGER,
    returned_at         REAL
);
"""


class Store:
    """Locking SQLite wrapper with snapshot load/save."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self._connection = sqlite3.connect(
            str(self.path), check_same_thread=False, isolation_level=None
        )
        self._connection.row_factory = sqlite3.Row
        with self.lock:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            self._connection.executescript(SCHEMA)

    def load_shares(self) -> dict[str, Share]:
        with self.lock:
            rows = self._connection.execute("SELECT * FROM shares").fetchall()
        return {
            row["name"]: Share(
                name=row["name"],
                nominal_mb=row["nominal_mb"],
                cap_mb=row["cap_mb"],
                reserved_mb=row["reserved_mb"],
            )
            for row in rows
        }

    def load_jobs(self) -> dict[str, Job]:
        with self.lock:
            rows = self._connection.execute(
                "SELECT * FROM jobs ORDER BY submitted_at"
            ).fetchall()
        return {
            row["job_id"]: Job(
                job_id=row["job_id"],
                share=row["share"],
                name=row["name"],
                memory_mb=row["memory_mb"],
                duration_seconds=row["duration_seconds"],
                status=row["status"],
                submitted_at=row["submitted_at"],
                started_at=row["started_at"],
                ends_at=row["ends_at"],
                finished_at=row["finished_at"],
                worker_endpoint=row["worker"],
                attempts=row["attempts"],
                last_contact_at=row["last_contact_at"],
                detail=row["detail"],
            )
            for row in rows
        }

    def load_loans(self) -> dict[str, Loan]:
        with self.lock:
            rows = self._connection.execute(
                "SELECT * FROM loans ORDER BY created_at"
            ).fetchall()
        loans: dict[str, Loan] = {}
        for row in rows:
            loan = Loan(
                loan_id=row["loan_id"],
                amount_mb=row["amount_mb"],
                duration_seconds=row["duration_seconds"],
                lender=row["lender"],
                borrower=row["borrower"],
                created_at=row["created_at"],
                release_started_at=row["release_started_at"],
                note=row["note"] or "",
                lender_occupancy_at_start=row["lender_occupancy_at_start"],
                status=LoanStatus(row["status"]),
                transferred_at=row["transferred_at"],
                due_at=row["due_at"],
                return_started_at=row["return_started_at"],
                borrower_occupancy_at_return_start=row[
                    "borrower_occupancy_at_return_start"
                ],
                returned_at=row["returned_at"],
            )
            loans[loan.loan_id] = loan
        return loans

    def save_snapshot(
        self,
        shares: dict[str, Share],
        jobs: dict[str, Job],
        loans: dict[str, Loan],
    ) -> None:
        share_rows = [
            (share.name, share.nominal_mb, share.cap_mb, share.reserved_mb)
            for share in shares.values()
        ]
        job_rows = [
            (
                job.job_id,
                job.share,
                job.name,
                job.memory_mb,
                job.duration_seconds,
                job.status,
                job.submitted_at,
                job.started_at,
                job.ends_at,
                job.finished_at,
                job.worker_endpoint,
                job.attempts,
                job.last_contact_at,
                job.detail,
            )
            for job in jobs.values()
        ]
        loan_rows = [
            (
                loan.loan_id,
                loan.amount_mb,
                loan.duration_seconds,
                getattr(loan, "note", "") or "",
                loan.lender,
                loan.borrower,
                loan.created_at,
                loan.release_started_at,
                loan.lender_occupancy_at_start,
                loan.status.value,
                loan.transferred_at,
                loan.due_at,
                loan.return_started_at,
                loan.borrower_occupancy_at_return_start,
                loan.returned_at,
            )
            for loan in loans.values()
        ]

        with self.lock:
            cursor = self._connection.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
            except sqlite3.Error as exc:
                raise StorageError(
                    f"could not start a database transaction: {exc}"
                ) from exc
            try:
                cursor.execute("DELETE FROM shares")
                cursor.executemany(
                    "INSERT INTO shares (name, nominal_mb, cap_mb, reserved_mb) "
                    "VALUES (?, ?, ?, ?)",
                    share_rows,
                )
                cursor.execute("DELETE FROM jobs")
                cursor.executemany(
                    "INSERT INTO jobs (job_id, share, name, memory_mb, "
                    "duration_seconds, status, submitted_at, started_at, ends_at, "
                    "finished_at, worker, attempts, last_contact_at, detail) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    job_rows,
                )
                cursor.execute("DELETE FROM loans")
                cursor.executemany(
                    "INSERT INTO loans (loan_id, amount_mb, duration_seconds, note, "
                    "lender, borrower, created_at, release_started_at, "
                    "lender_occupancy_at_start, status, transferred_at, due_at, "
                    "return_started_at, borrower_occupancy_at_return_start, "
                    "returned_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    loan_rows,
                )
                cursor.execute("COMMIT")
            except Exception as exc:
                try:
                    cursor.execute("ROLLBACK")
                except sqlite3.Error:  # pragma: no cover - connection already broken
                    # The connection may be left inside a transaction (e.g. a
                    # disk I/O error).  Reopen it so the next attempt starts from
                    # a clean, retryable state instead of failing forever.  A
                    # reconnect failure must not mask the StorageError below.
                    try:
                        self._reconnect()
                    except sqlite3.Error:
                        logger.warning("could not reopen the database connection")
                # The transaction rolled back: the database still holds the last
                # consistent snapshot, so the caller can safely retry.
                raise StorageError(f"could not save scheduler state: {exc}") from exc

    def _reconnect(self) -> None:  # pragma: no cover - error recovery path
        try:
            self._connection.close()
        except sqlite3.Error:
            pass
        self._connection = sqlite3.connect(
            str(self.path), check_same_thread=False, isolation_level=None
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")

    def clear(self) -> None:
        with self.lock:
            cursor = self._connection.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("DELETE FROM shares")
            cursor.execute("DELETE FROM jobs")
            cursor.execute("DELETE FROM loans")
            cursor.execute("COMMIT")

    def close(self) -> None:
        with self.lock:
            self._connection.close()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Store({json.dumps(str(self.path))})"
