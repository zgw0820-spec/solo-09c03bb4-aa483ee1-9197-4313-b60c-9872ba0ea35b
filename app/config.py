"""Application settings, loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:  # pragma: no cover - startup guard
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:  # pragma: no cover - startup guard
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


@dataclass(frozen=True)
class Settings:
    """Static configuration for one scheduler node."""

    node_id: str
    total_memory_mb: int
    regular_share_mb: int
    patch_share_mb: int
    regular_worker_url: str
    patch_worker_url: str
    tick_interval_seconds: float
    worker_timeout_seconds: float
    probe_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "Settings":
        node_id = os.environ.get("NODE_ID", "node-a").strip() or "node-a"
        total = _int_env("TOTAL_MEMORY_MB", 4096)
        regular = _int_env("REGULAR_SHARE_MB", 3072)
        patch = _int_env("PATCH_SHARE_MB", 1024)
        if regular + patch != total:
            raise ValueError(
                "REGULAR_SHARE_MB + PATCH_SHARE_MB must equal TOTAL_MEMORY_MB "
                f"({regular} + {patch} != {total})"
            )
        return cls(
            node_id=node_id,
            total_memory_mb=total,
            regular_share_mb=regular,
            patch_share_mb=patch,
            regular_worker_url=(
                os.environ.get("REGULAR_WORKER_URL", "http://127.0.0.1:8011").strip()
            ),
            patch_worker_url=(
                os.environ.get("PATCH_WORKER_URL", "http://127.0.0.1:8012").strip()
            ),
            tick_interval_seconds=_float_env("TICK_INTERVAL_SECONDS", 0.2),
            worker_timeout_seconds=_float_env("WORKER_TIMEOUT_SECONDS", 2.0),
            probe_timeout_seconds=_float_env("PROBE_TIMEOUT_SECONDS", 2.0),
        )


def get_settings() -> Settings:
    """Return settings built from the current environment (not cached for tests)."""

    return Settings.from_env()
