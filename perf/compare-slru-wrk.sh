#!/usr/bin/env sh
set -eu

cd /mnt/e/goCache

mkdir -p .gocache-linux .gotmp-linux test-logs/slru-wrk

server_bin="test-logs/slru-wrk/cache-server-linux"
GOCACHE=/mnt/e/goCache/.gocache-linux \
GOTMPDIR=/mnt/e/goCache/.gotmp-linux \
go build -o "$server_bin" ./cmd/cache-server

duration="${WRK_DURATION:-20s}"
threads="${WRK_THREADS:-8}"
connections="${WRK_CONNECTIONS:-400}"
cache_bytes="${CACHE_BYTES:-1048576}"
shards="${WRK_SHARDS:-256}"
latency_sample_rate="${LATENCY_SAMPLE_RATE:-0}"
hot_keys="${HOT_KEYS:-1}"
hot_repeats="${HOT_REPEATS:-2}"
cold_burst="${SLRU_SCAN_BURST:-32}"

warm_hot_keys() {
  api_port="$1"
  rounds="${HOT_WARMUP_ROUNDS:-2}"
  round=0
  while [ "$round" -lt "$rounds" ]; do
    i=0
    while [ "$i" -lt "$hot_keys" ]; do
      curl -fsS "http://127.0.0.1:$api_port/api?key=key$i" >/dev/null
      i=$((i + 1))
    done
    round=$((round + 1))
  done
}

run_case() {
  strategy="$1"
  peer_port="$2"
  api_port="$3"
  out_dir="test-logs/slru-wrk/$strategy"
  mkdir -p "$out_dir"

  "$server_bin" \
    -port="$peer_port" \
    -api=true \
    -api-addr="127.0.0.1:$api_port" \
    -self-addr="http://127.0.0.1:$peer_port" \
    -peers="http://127.0.0.1:$peer_port" \
    -strategy="$strategy" \
    -shards="$shards" \
    -backend=synthetic \
    -cache-bytes="$cache_bytes" \
    -eviction-log=false \
    -latency-sample-rate="$latency_sample_rate" \
    > "$out_dir/server.out.log" \
    2> "$out_dir/server.err.log" &

  pid="$!"
  ready=0
  i=0
  while [ "$i" -lt 100 ]; do
    if curl -fsS "http://127.0.0.1:$api_port/ping" >/dev/null 2>&1; then
      ready=1
      break
    fi
    i=$((i + 1))
    sleep 0.2
  done

  if [ "$ready" -ne 1 ]; then
    echo "server_not_ready strategy=$strategy" >&2
    cat "$out_dir/server.err.log" >&2 || true
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    exit 1
  fi

  warm_hot_keys "$api_port"
  curl -fsS "http://127.0.0.1:$api_port/debug/stats" > "$out_dir/before.json"

  echo "===== $strategy scan workload hot_keys=$hot_keys hot_repeats=$hot_repeats cold_burst=$cold_burst cache_bytes=$cache_bytes shards=$shards ====="
  HOT_KEYS="$hot_keys" HOT_REPEATS="$hot_repeats" SLRU_SCAN_BURST="$cold_burst" \
    wrk -t"$threads" -c"$connections" -d"$duration" --latency \
    -s perf/wrk-slru-scan.lua \
    "http://127.0.0.1:$api_port/api" \
    | tee "$out_dir/wrk.log"

  curl -fsS "http://127.0.0.1:$api_port/debug/stats" > "$out_dir/after.json"

  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

run_case sharded 19101 29991
run_case sharded-slru 19102 29992

python3 - <<'PY'
import json
import pathlib
import re

root = pathlib.Path("test-logs/slru-wrk")

def load_case(name):
    before = json.loads((root / name / "before.json").read_text())
    after = json.loads((root / name / "after.json").read_text())
    wrk = (root / name / "wrk.log").read_text(errors="replace")
    requests = re.search(r"Requests/sec:\s+([0-9.]+)", wrk)
    latency = re.search(r"Latency\s+([0-9.]+)(us|ms|s)", wrk)
    p99 = re.search(r"\s+99%\s+([0-9.]+)(us|ms|s)", wrk)

    def to_ms(match):
        if not match:
            return None
        value = float(match.group(1))
        unit = match.group(2)
        if unit == "us":
            return value / 1000.0
        if unit == "s":
            return value * 1000.0
        return value

    delta_totals = {
        key: after["totals"].get(key, 0) - before["totals"].get(key, 0)
        for key in after["totals"]
    }
    rates = after.get("rates", {})
    group_gets = delta_totals.get("group_gets", 0)
    cache_misses = delta_totals.get("cache_misses", 0)
    local_loads = delta_totals.get("local_loads", 0)
    return {
        "totals": delta_totals,
        "hit_rate_percent": rates.get("hit_rate_percent", 0.0),
        "miss_rate_percent": (cache_misses / group_gets * 100.0) if group_gets else 0.0,
        "local_load_rate_percent": (local_loads / group_gets * 100.0) if group_gets else 0.0,
        "api_p95_ms": rates.get("api_p95_ms", 0.0),
        "api_p99_ms": rates.get("api_p99_ms", 0.0),
        "wrk_requests_sec": float(requests.group(1)) if requests else None,
        "wrk_avg_latency_ms": to_ms(latency),
        "wrk_p99_ms": to_ms(p99),
    }

base = load_case("sharded")
slru = load_case("sharded-slru")

def reduction(old, new):
    if old <= 0:
        return 0.0
    return (old - new) / old * 100.0

print("===== summary =====")
for name, data in [("sharded", base), ("sharded-slru", slru)]:
    totals = data["totals"]
    print(
        f"{name}: hits={totals.get('cache_hits', 0)} "
        f"misses={totals.get('cache_misses', 0)} "
        f"local_loads={totals.get('local_loads', 0)} "
        f"hit_rate={data['hit_rate_percent']:.2f}% "
        f"miss_rate={data['miss_rate_percent']:.2f}% "
        f"local_load_rate={data['local_load_rate_percent']:.2f}% "
        f"rps={data['wrk_requests_sec']} "
        f"wrk_avg_ms={data['wrk_avg_latency_ms']} "
        f"wrk_p99_ms={data['wrk_p99_ms']} "
        f"api_p95_ms={data['api_p95_ms']} "
        f"api_p99_ms={data['api_p99_ms']}"
    )

print(
    "improvement: "
    f"cache_misses_reduction={reduction(base['totals'].get('cache_misses', 0), slru['totals'].get('cache_misses', 0)):.2f}% "
    f"local_loads_reduction={reduction(base['totals'].get('local_loads', 0), slru['totals'].get('local_loads', 0)):.2f}% "
    f"miss_rate_reduction={reduction(base['miss_rate_percent'], slru['miss_rate_percent']):.2f}% "
    f"local_load_rate_reduction={reduction(base['local_load_rate_percent'], slru['local_load_rate_percent']):.2f}%"
)
PY
