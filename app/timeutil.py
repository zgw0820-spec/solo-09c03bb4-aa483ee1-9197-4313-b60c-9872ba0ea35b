"""Small shared helpers."""

from __future__ import annotations

import time


def iso(timestamp: float | None) -> str | None:
    """Render a POSIX timestamp as an ISO-8601 UTC string with milliseconds."""

    if timestamp is None:
        return None
    seconds = int(timestamp)
    millis = int(round((timestamp - seconds) * 1000))
    if millis == 1000:  # rounding carried into the next second
        seconds += 1
        millis = 0
    base = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds))
    return f"{base}.{millis:03d}Z"
