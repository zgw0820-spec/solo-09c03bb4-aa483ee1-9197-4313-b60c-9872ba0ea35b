# Replay patch-transcode memory scheduler

When a live-replay recording is missing segments, the **patch re-transcode
queue** needs extra memory quickly.  That memory is borrowed from the
machine's **regular transcode queue**.  Both queues are served by separate
mock workers in Docker Compose and share one fixed memory budget.

The scheduler API (FastAPI + Swagger UI) is the single authority that:

* admits transcode tasks against a queue's **effective** ceiling,
* moves capacity between the two queues via time-boxed **loans**,
* never lets the two effective ceilings sum to more than the node budget,
* never revokes capacity already granted to an admitted task.

## Quick start (Docker Compose)

```bash
docker compose up --build
```

| Service         | URL                            |
| --------------- | ------------------------------ |
| Scheduler Swagger UI | http://localhost:8000/docs |
| Scheduler OpenAPI    | http://localhost:8000/openapi.json |
| Regular worker health | http://localhost:8020/health |
| Patch worker health   | http://localhost:8021/health |

Budget split (all configurable via environment):

```
TOTAL_MEMORY_MB=4096   REGULAR_SHARE_MB=3072   PATCH_SHARE_MB=1024
```

## The operator workflow

All steps are available from Swagger (`/docs`).

1. **Submit work** — `POST /jobs`

   ```json
   {"share": "patch", "memory_mb": 512, "duration_seconds": 60,
    "name": "replay-2026-11-21-1930"}
   ```

   `share` is `regular` (normal/live) or `patch` (the missing-segment
   re-transcode queue).  A task is admitted only if its memory fits the queue's
   current free memory under its *effective* ceiling.  Otherwise `422`.

   Submission is all-or-nothing: the job and its reservation become visible only
   after the SQLite snapshot is saved.  If the database cannot be written the
   request returns `503` (nothing is admitted, nothing is dispatched) and the
   same request can be retried once storage is back.

2. **Borrow capacity** — `POST /loans`

   ```json
   {"amount_mb": 512, "duration_seconds": 300,
    "note": "incident 42, replay segments 3-9 missing"}
   ```

   Only one loan may be outstanding at a time; a second request gets `409` until
   the first is fully returned.  `amount_mb` may not exceed the regular share's
   nominal ceiling.

   The loan request is all-or-nothing as well: the lender's cap reduction, any
   immediate handover to the borrower, and the loan record are saved in one
   transaction.  If the save fails the request returns `503`, every cap is
   restored and no loan (or outstanding-loan block) remains, so nothing that was
   never confirmed can influence later admissions or be written by a later tick.

3. **Watch** — `GET /capacity`, `GET /loans/{loan_id}` (or
   `GET /loans/{loan_id}/progress`).

4. **Abort a loan that has not started** — `DELETE /loans/{loan_id}` works only
   while the loan is still `releasing` (capacity has not moved).  Once the
   capacity is with the borrower it can only come back through the normal
   drain; the endpoint returns `409`.

### Loan state machine

```
                 lender occupancy drains
                     to reduced cap            duration elapses
  releasing ─────────────────────────────▶ active ──────────────┐
  (ceiling cut, timer NOT started)         (timer running)      │
                                                                ▼
                                              returned ◀──── returning
                                            (cap back with     (borrower ceiling
                                             lender)            cut, waits for
                                                                occupancy to drain)
```

* On request the **lender's** effective cap drops immediately.  It keeps
  serving its already admitted tasks, but accepts no new work above the
  reduced cap.  The timer has **not** started yet.
* Only when the lender's committed memory has drained to the reduced cap is the
  capacity added to the **borrower's** effective cap — that is when the loan
  timer starts, so the full requested duration is usable.
* On expiry the borrower's cap drops back immediately.  Already running jobs
  continue; new work is refused above the cap.
* The capacity returns to the lender only once the borrower's committed memory
  has drained to its nominal cap.

Throughout, `regular.cap + patch.cap <= TOTAL_MEMORY_MB`, and committed memory
never exceeds the node budget.  `GET /capacity` and the loan view expose
`within_budget`, `over_cap_mb` and `*_drain_needed_mb` so the progress is
visible.

## Architecture

```
             ┌──────────────────────────────┐
  Swagger ──▶│ scheduler (FastAPI, :8000)   │
             │  allocation.py  pure math    │
             │  scheduler.py   tick/dispatch│
             │  store.py       SQLite       │
             └───────┬──────────────┬───────┘
                     │ POST /jobs   │ POST /jobs
                     ▼              ▼
             regular-worker     patch-worker
               (:8020)            (:8020)
```

* The scheduler runs a background ticker (`TICK_INTERVAL_SECONDS`, default
  0.2 s) that dispatches queued jobs, polls running jobs, and advances loans.
* State is persisted to SQLite after every mutation (`DATABASE_PATH`,
  default `./data/scheduler.db`; `/data/scheduler.db` in Compose, backed by a
  named volume).  On restart, caps, jobs and the outstanding loan are restored,
  and running jobs are re-polled from the workers.
* The mock workers are deliberately dumb: they hold memory for the requested
  duration and report completion.  They are stopped by `docker compose down`.

## Running without Docker

```bash
python3 -m pip install -r requirements-dev.txt
./scripts/live_run.sh up        # workers + scheduler with uvicorn
python3 scripts/live_scenario.py  # scripted walkthrough of the whole flow
./scripts/live_run.sh down
./scripts/live_run.sh reset     # also delete the SQLite state
```

## Tests

```bash
python3 -m pytest -q
```

* `tests/test_allocation.py` — pure capacity arithmetic against a controlled
  clock: handover only after drain, timer starts at handover, expiry cuts the
  cap then waits for drain, caps never exceed the budget.
* `tests/test_api.py` — scheduler + FastAPI + both mock workers (in-process
  `httpx.MockTransport`) with a fake clock: rejection beyond the cap,
  in-flight jobs survive loan expiry, one-loan-at-a-time, persistence across
  restart, Swagger/OpenAPI smoke test.
* `tests/test_scheduler_edges.py` — aborted loans, a "promise" loan larger than
  the current free memory, restart with a changed budget, unknown shares.
* `tests/test_storage_failures.py` — a failed snapshot write at either entry
  point rolls the in-memory change back, returns `503`, cannot be persisted by a
  later tick or influence admission, and succeeds once storage recovers.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `NODE_ID` | `node-a` | Node label in responses |
| `TOTAL_MEMORY_MB` | `4096` | Fixed node budget |
| `REGULAR_SHARE_MB` | `3072` | Nominal regular ceiling |
| `PATCH_SHARE_MB` | `1024` | Nominal patch ceiling |
| `REGULAR_WORKER_URL` | `http://127.0.0.1:8011` | Regular mock worker |
| `PATCH_WORKER_URL` | `http://127.0.0.1:8012` | Patch mock worker |
| `TICK_INTERVAL_SECONDS` | `0.2` | Dispatch/poll/loan cadence |
| `WORKER_TIMEOUT_SECONDS` | `2.0` | Per worker HTTP timeout |
| `PROBE_TIMEOUT_SECONDS` | `2.0` | `/capacity` probe timeout || `DATABASE_PATH` | `./data/scheduler.db` | SQLite file |

`REGULAR_SHARE_MB + PATCH_SHARE_MB` must equal `TOTAL_MEMORY_MB`.

## Known limitations / next steps

* Single scheduler process per node; SQLite is not a distributed coordinator.
  Multiple schedulers on one budget would need a shared transactional store
  (e.g. Postgres row locks) or a leader election.
* Loans are always regular → patch and amount is bounded by the *nominal*
  regular ceiling, not by what is free right now.  That is intentional: the
  loan waits for the lender to drain.  A loan can be aborted only while it is
  still `releasing` (`DELETE /loans/{loan_id}`).
* If a worker dies while holding a job, the scheduler requeues the job when the
  worker returns 404, but there is no lease/heartbeat expiry for a worker that
  is simply unreachable.  The memory stays reserved until the worker is back or
  an operator reconciles manually.
* Job completion is polled; there is no push/callback from workers, so a job's
  recorded end time can lag by up to one tick.
* No authentication; the API is meant to sit behind the on-call network.
