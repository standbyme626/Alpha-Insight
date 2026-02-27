#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
DB_PATH="${TELEGRAM_GATEWAY_DB:-storage/telegram_gateway_live.db}"
POLL_TIMEOUT_SECONDS="${TELEGRAM_POLL_TIMEOUT_SECONDS:-15}"
IDLE_SLEEP_SECONDS="${TELEGRAM_IDLE_SLEEP_SECONDS:-0.3}"
SCHED_POLL_INTERVAL="${TELEGRAM_SCHEDULER_POLL_INTERVAL_SECONDS:-1}"
SCHED_BATCH_SIZE="${TELEGRAM_SCHEDULER_BATCH_SIZE:-20}"
LOG_DIR="${TELEGRAM_STACK_LOG_DIR:-/tmp}"
GATEWAY_LOG="$LOG_DIR/telegram_gateway.log"
SCHED_LOG="$LOG_DIR/telegram_scheduler.log"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a && source "$ENV_FILE" && set +a
fi

export PYTHONPATH="${PYTHONPATH:-$ROOT_DIR}"

if [[ -z "${TELEGRAM_BOT_TOKEN:-}" ]]; then
  echo "TELEGRAM_BOT_TOKEN is required (from .env or env vars)." >&2
  exit 1
fi

gateway_pattern="python -u scripts/telegram_long_polling_gateway.py"
scheduler_pattern="python -u scripts/telegram_watch_scheduler.py"

get_pids() {
  local pattern="$1"
  pgrep -f "$pattern" || true
}

start_gateway() {
  if [[ -n "$(get_pids "$gateway_pattern")" ]]; then
    echo "gateway already running: $(get_pids "$gateway_pattern" | tr '\n' ' ')"
    return
  fi
  TELEGRAM_GATEWAY_DB="$DB_PATH" \
    setsid bash -lc \
    "cd '$ROOT_DIR' && exec python -u scripts/telegram_long_polling_gateway.py --db-path '$DB_PATH' --poll-timeout-seconds '$POLL_TIMEOUT_SECONDS' --idle-sleep-seconds '$IDLE_SLEEP_SECONDS'" \
    >>"$GATEWAY_LOG" 2>&1 < /dev/null &
  sleep 1
  echo "gateway started: $(get_pids "$gateway_pattern" | tr '\n' ' ')"
}

start_scheduler() {
  if [[ -n "$(get_pids "$scheduler_pattern")" ]]; then
    echo "scheduler already running: $(get_pids "$scheduler_pattern" | tr '\n' ' ')"
    return
  fi
  TELEGRAM_GATEWAY_DB="$DB_PATH" \
    setsid bash -lc \
    "cd '$ROOT_DIR' && exec python -u scripts/telegram_watch_scheduler.py --db-path '$DB_PATH' --poll-interval-seconds '$SCHED_POLL_INTERVAL' --batch-size '$SCHED_BATCH_SIZE'" \
    >>"$SCHED_LOG" 2>&1 < /dev/null &
  sleep 1
  echo "scheduler started: $(get_pids "$scheduler_pattern" | tr '\n' ' ')"
}

stop_by_pattern() {
  local name="$1"
  local pattern="$2"
  local pids
  pids="$(get_pids "$pattern" | tr '\n' ' ')"
  if [[ -z "${pids// }" ]]; then
    echo "$name already stopped"
    return
  fi
  kill $pids
  sleep 1
  local remain
  remain="$(get_pids "$pattern" | tr '\n' ' ')"
  if [[ -n "${remain// }" ]]; then
    kill -9 $remain
  fi
  echo "$name stopped"
}

status() {
  local gpids spids
  gpids="$(get_pids "$gateway_pattern" | tr '\n' ' ')"
  spids="$(get_pids "$scheduler_pattern" | tr '\n' ' ')"
  echo "gateway: ${gpids:-stopped}"
  echo "scheduler: ${spids:-stopped}"
  echo "db_path: $DB_PATH"
  echo "logs: $GATEWAY_LOG | $SCHED_LOG"
}

cmd="${1:-status}"
case "$cmd" in
  start)
    start_gateway
    start_scheduler
    status
    ;;
  stop)
    stop_by_pattern "gateway" "$gateway_pattern"
    stop_by_pattern "scheduler" "$scheduler_pattern"
    status
    ;;
  restart)
    stop_by_pattern "gateway" "$gateway_pattern"
    stop_by_pattern "scheduler" "$scheduler_pattern"
    start_gateway
    start_scheduler
    status
    ;;
  status)
    status
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}" >&2
    exit 2
    ;;
esac
