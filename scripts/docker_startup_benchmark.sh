#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required"
  exit 1
fi

echo "[STEP] Cold build (no cache)"
start_cold=$(date +%s)
docker compose build --no-cache dev >/tmp/alpha_build_cold.log 2>&1
end_cold=$(date +%s)

cold_run_start=$(date +%s)
docker compose run --rm dev python -c "import pandas,yfinance,langgraph,aiohttp;print('deps ok')" >/tmp/alpha_run_cold.log 2>&1
cold_run_end=$(date +%s)

echo "[STEP] Hot start (cached image + pip cache volume)"
hot_run_start=$(date +%s)
docker compose run --rm dev python -c "import pandas,yfinance,langgraph,aiohttp;print('deps ok')" >/tmp/alpha_run_hot.log 2>&1
hot_run_end=$(date +%s)

cat <<EOF
Cold build time: $((end_cold - start_cold))s
Cold run time:   $((cold_run_end - cold_run_start))s
Hot run time:    $((hot_run_end - hot_run_start))s
Logs:
  /tmp/alpha_build_cold.log
  /tmp/alpha_run_cold.log
  /tmp/alpha_run_hot.log
EOF
