#!/usr/bin/env sh
set -eu

cd /mnt/e/goCache

mkdir -p .gocache-linux .gotmp-linux test-logs/decomp-wsl

GOCACHE=/mnt/e/goCache/.gocache-linux \
GOTMPDIR=/mnt/e/goCache/.gotmp-linux \
go build -o test-logs/decomp-wsl/decomp-server-linux ./main

pkill -f decomp-server-linux 2>/dev/null || true

./test-logs/decomp-wsl/decomp-server-linux \
  -port=19001 \
  -api=true \
  -api-addr=127.0.0.1:29999 \
  -self-addr=http://127.0.0.1:19001 \
  -peers=http://127.0.0.1:19001 \
  -strategy=sharded \
  -latency-sample-rate=0 \
  > test-logs/decomp-wsl/server.out.log \
  2> test-logs/decomp-wsl/server.err.log &

pid=$!

cleanup() {
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

i=0
while [ "$i" -lt 60 ]; do
  if curl -fsS http://127.0.0.1:29999/ping >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.2
done

if [ "$i" -ge 60 ]; then
  echo "server_not_ready"
  cat test-logs/decomp-wsl/server.err.log
  exit 1
fi

curl -fsS "http://127.0.0.1:29999/api?key=Tom" >/dev/null

duration="${WRK_DURATION:-8s}"
threads="${WRK_THREADS:-8}"

for conn in 400 800; do
  echo "===== ping c=$conn ====="
  wrk -t"$threads" -c"$conn" -d"$duration" --latency \
    http://127.0.0.1:29999/ping \
    | tee "test-logs/decomp-wsl/ping-c$conn.log"

  echo "===== api-local c=$conn ====="
  wrk -t"$threads" -c"$conn" -d"$duration" --latency \
    "http://127.0.0.1:29999/api?key=Tom" \
    | tee "test-logs/decomp-wsl/api-local-c$conn.log"

  echo "===== peer-rpc c=$conn ====="
  wrk -t"$threads" -c"$conn" -d"$duration" --latency \
    -s perf/wrk-peer-rpc.lua \
    http://127.0.0.1:19001/_goCache/get \
    | tee "test-logs/decomp-wsl/peer-rpc-c$conn.log"
done
