"""Live demo against a running stack (``scripts/live_run.sh up``).

Walks through the full incident flow and prints the state transitions:

1. fill the regular queue with a long job;
2. request a loan while the lender is still busy (loan waits, cap already cut);
3. let the lender drain -> handover, timer starts;
4. run a patch job using the borrowed capacity;
5. let the loan expire while the patch job is still running (cap drops, job
   keeps running, capacity is not returned yet);
6. let the patch job finish -> capacity returns.
"""

from __future__ import annotations

import sys
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
client = httpx.Client(base_url=BASE, timeout=10)


def show(label: str) -> dict:
    capacity = client.get("/capacity").json()
    shares = {s["name"]: s for s in capacity["shares"]}
    loan_id = capacity["outstanding_loan_id"]
    loan = client.get(f"/loans/{loan_id}").json() if loan_id else None
    print(f"\n--- {label} ---")
    print(
        f"regular cap={shares['regular']['cap_mb']:5d} "
        f"reserved={shares['regular']['reserved_mb']:5d} | "
        f"patch cap={shares['patch']['cap_mb']:5d} "
        f"reserved={shares['patch']['reserved_mb']:5d} | "
        f"sum={capacity['effective_cap_sum_mb']} within_budget={capacity['within_budget']}"
    )
    if loan:
        print(
            f"loan {loan['loan_id']} {loan['status']:9s} amount={loan['amount_mb']} "
            f"remaining={loan['remaining_seconds']} "
            f"lender_drain_needed={loan['lender_drain_needed_mb']} "
            f"borrower_drain_needed={loan['borrower_drain_needed_mb']}"
        )
        print(f"      {loan['progress']}")
    return capacity


def wait_for(description: str, predicate, timeout: float = 60.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        capacity = client.get("/capacity").json()
        if predicate(capacity):
            print(f"  [ok] {description}")
            return capacity
        time.sleep(0.25)
    raise SystemExit(f"timed out waiting for: {description}")


def share(capacity: dict, name: str) -> dict:
    return next(item for item in capacity["shares"] if item["name"] == name)


def loan_of(capacity: dict) -> dict | None:
    loan_id = capacity["outstanding_loan_id"]
    return client.get(f"/loans/{loan_id}").json() if loan_id else None


def main() -> None:
    show("initial")

    lender = client.post(
        "/jobs",
        json={"share": "regular", "memory_mb": 3072, "duration_seconds": 9, "name": "live-fill"},
    ).json()
    print(f"\nsubmitted regular fill job {lender['job_id']}")
    wait_for(
        "regular fill job is running",
        lambda c: share(c, "regular")["reserved_mb"] == 3072,
    )

    rejected = client.post(
        "/jobs", json={"share": "regular", "memory_mb": 1, "duration_seconds": 5}
    )
    assert rejected.status_code == 422, rejected.text
    print(f"  [ok] regular queue rejected extra work: {rejected.json()['detail']}")

    loan = client.post(
        "/loans", json={"amount_mb": 512, "duration_seconds": 6, "note": "missing segments"}
    ).json()
    print(f"\nloan {loan['loan_id']} requested while lender is full")
    show("right after loan request (releasing, timer not started)")

    second = client.post("/loans", json={"amount_mb": 64, "duration_seconds": 5})
    assert second.status_code == 409, second.text
    print(f"  [ok] second loan refused: {second.json()['detail']}")

    wait_for(
        "handover: patch cap is 1536, loan active",
        lambda c: share(c, "patch")["cap_mb"] == 1536
        and (loan_of(c) or {}).get("status") == "active",
        timeout=30,
    )
    show("after handover (timer started)")

    patch_job = client.post(
        "/jobs",
        json={
            "share": "patch",
            "memory_mb": 1536,
            "duration_seconds": 8,
            "name": "replay-patch",
        },
    ).json()
    print(f"\nsubmitted patch job {patch_job['job_id']} using the borrowed capacity")
    wait_for(
        "patch job is running at 1536 MB",
        lambda c: share(c, "patch")["reserved_mb"] == 1536,
    )

    wait_for(
        "loan expired while patch job still runs (returning)",
        lambda c: (loan_of(c) or {}).get("status") == "returning",
        timeout=30,
    )
    show("at expiry: cap dropped, job still running, capacity NOT yet returned")

    job = client.get(f"/jobs/{patch_job['job_id']}").json()
    assert job["status"] == "running", job
    print(f"  [ok] patch job {job['job_id']} still {job['status']} with "
          f"{job['remaining_seconds']}s left")

    wait_for(
        "patch job finished and capacity returned",
        lambda c: (loan_of(c) or {}).get("status") == "returned"
        or c["outstanding_loan_id"] is None,
        timeout=30,
    )
    show("after return")

    final = client.get("/capacity").json()
    final_shares = {s["name"]: s for s in final["shares"]}
    assert final_shares["regular"]["cap_mb"] == 3072
    assert final_shares["patch"]["cap_mb"] == 1024
    assert final["within_budget"]
    print("\nfinal caps restored: regular=3072 patch=1024, within budget ✔")


if __name__ == "__main__":
    main()
