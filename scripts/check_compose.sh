#!/usr/bin/env bash
# Validate docker-compose.yml syntax if Docker is available.
# Falls back to a light YAML parse so CI without Docker still catches typos.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    docker compose config --quiet
    echo "docker compose config: ok"
else
    python3 - <<'PY'
import sys

try:
    import yaml
except ImportError:  # pragma: no cover - PyYAML is optional
    print("PyYAML not installed; skipping YAML validation")
    sys.exit(0)

with open("docker-compose.yml", encoding="utf-8") as handle:
    config = yaml.safe_load(handle)

services = config.get("services") or {}
for required in ("scheduler", "regular-worker", "patch-worker"):
    if required not in services:
        raise SystemExit(f"missing service: {required}")
scheduler = services["scheduler"]
env = scheduler.get("environment", {})
total = int(env["TOTAL_MEMORY_MB"])
shares = int(env["REGULAR_SHARE_MB"]) + int(env["PATCH_SHARE_MB"])
if total != shares:
    raise SystemExit(f"budget mismatch: {total} != {shares}")
print("compose structure: ok (YAML only; Docker not available)")
PY
fi
