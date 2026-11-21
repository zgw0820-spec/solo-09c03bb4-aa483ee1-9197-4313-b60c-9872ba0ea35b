#!/usr/bin/env bash
# Start the scheduler and two mock workers with plain uvicorn (no Docker).
# Used for local smoke runs; Docker Compose is the supported deployment.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${RUN_DIR:-/tmp/replay-scheduler-live}"
PYTHON="${PYTHON:-python3}"

mkdir -p "$RUN_DIR"
cd "$ROOT"

start() {
    local name="$1" port="$2" module="$3"
    local -a factory=()
    if [[ "$module" == *":create_app" ]]; then
        factory=(--factory)
    fi
    local log="$RUN_DIR/$name.log"
    local pidfile="$RUN_DIR/$name.pid"
    if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "$name already running (pid $(cat "$pidfile"))"
        return
    fi
    WORKER_NAME="$name" nohup "$PYTHON" -m uvicorn "$module" "${factory[@]}" \
        --host 127.0.0.1 --port "$port" --log-level warning \
        >"$log" 2>&1 &
    echo $! >"$pidfile"
    echo "started $name on :$port (pid $(cat "$pidfile"), log $log)"
}

stop_all() {
    for pidfile in "$RUN_DIR"/*.pid; do
        [[ -e "$pidfile" ]] || continue
        pid="$(cat "$pidfile")"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            echo "stopped pid $pid ($(basename "$pidfile"))"
        fi
        rm -f "$pidfile"
    done
}

wait_for() {
    local url="$1" tries=50
    for _ in $(seq "$tries"); do
        if curl -fsS "$url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.2
    done
    echo "timed out waiting for $url" >&2
    return 1
}

case "${1:-up}" in
    up)
        start regular-worker 8011 worker.main:app
        start patch-worker 8012 worker.main:app
        wait_for http://127.0.0.1:8011/health
        wait_for http://127.0.0.1:8012/health
        export NODE_ID="${NODE_ID:-local-dev}" \
            TOTAL_MEMORY_MB="${TOTAL_MEMORY_MB:-4096}" \
            REGULAR_SHARE_MB="${REGULAR_SHARE_MB:-3072}" \
            PATCH_SHARE_MB="${PATCH_SHARE_MB:-1024}" \
            REGULAR_WORKER_URL=http://127.0.0.1:8011 \
            PATCH_WORKER_URL=http://127.0.0.1:8012 \
            DATABASE_PATH="$RUN_DIR/scheduler.db"
        start scheduler 8000 app.api:create_app
        wait_for http://127.0.0.1:8000/health
        echo "Swagger UI: http://127.0.0.1:8000/docs"
        ;;
    down)
        stop_all
        ;;
    reset)
        stop_all
        rm -f "$RUN_DIR"/scheduler.db*
        echo "state cleared in $RUN_DIR"
        ;;
    *)
        echo "usage: $0 {up|down|reset}" >&2
        exit 2
        ;;
esac
