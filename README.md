# cachecore

[![Go Report Card](https://goreportcard.com/badge/github.com/Thadopz/cachecore)](https://goreportcard.com/report/github.com/Thadopz/cachecore)

[English](README.md) | [简体中文](README.zh-CN.md)

`cachecore` is a Go distributed cache library and demo server inspired by
groupcache. It provides local LRU or SLRU caching, peer-to-peer value loading
over HTTP, singleflight request coalescing, optional Bloom filter pre-checks,
negative caching, TTL jitter, stale-while-revalidate fallback, and lightweight
runtime metrics.

The public Go package is:

```go
import cache "github.com/Thadopz/cachecore"
```

## Features

- In-memory LRU or SLRU cache with byte-size limits, TTL expiration, and eviction callbacks.
- Distributed peer routing with consistent hashing and protobuf-over-HTTP transport.
- Singleflight load coalescing for concurrent misses on the same key.
- Optional sharded cache mode for lower lock contention under parallel workloads.
- Optional SLRU policy through `WithSLRU`, useful when repeated hot keys should survive cold-key scans.
- Optional Bloom filter gate through `WithFilter` and the public `bloomfilter` package.
- Negative caching for `ErrNotFound`.
- Optional stale-while-revalidate fallback when waiting for an in-flight load times out.
- Built-in counters and latency sampling exposed by the demo server at `/debug/stats`.
- Redis-backed demo server, Dockerfile, Kubernetes manifests, and performance tools.

## Project Layout

```text
.
|-- cmd/cache-server/        # Redis-backed demo server and API
|-- bloomfilter/             # Public Bloom filter implementation for WithFilter
|-- deployments/k8s/         # Kubernetes manifests
|-- internal/consistenthash/ # Internal peer hash ring
|-- internal/groupcachepb/   # Internal protobuf transport types
|-- internal/lru/            # Internal LRU and SLRU cache policies
|-- internal/singleflight/   # Internal request coalescing
|-- perf/                    # Benchmark, k6, pprof, and wrk helpers
`-- *.go                     # Public cache package
```

## Library Usage

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	cache "github.com/Thadopz/cachecore"
)

func main() {
	g := cache.NewGroup("scores", 64<<20, cache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			if key == "Tom" {
				return []byte("630"), nil
			}
			return nil, cache.ErrNotFound
		}),
		cache.WithShardedCache(256),
		cache.WithSLRU(0.8),
		cache.WithRandomTTL(5*time.Minute, 30*time.Second),
		cache.WithNegativeCache(10*time.Second),
	)

	view, err := g.Get(context.Background(), "Tom")
	if err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			return
		}
		panic(err)
	}

	fmt.Println(view.String())
}
```

## Distributed Peers

Use `HTTPPool` when multiple cache nodes should route keys to their owner node:

```go
pool := cache.NewHTTPPool("http://localhost:8001")
pool.Set(
	"http://localhost:8001",
	"http://localhost:8002",
	"http://localhost:8003",
)
group.RegisterPeers(pool)
```

Serve the pool with `http.ListenAndServe`. The protobuf peer protocol stays
inside `internal/groupcachepb` and is not part of the public API.

## Demo Server

Build the server:

```powershell
go build ./cmd/cache-server
```

Run one local node with API enabled:

```powershell
go run ./cmd/cache-server -port=8001 -api=true -api-addr=0.0.0.0:9999
```

Common endpoints:

```powershell
curl "http://127.0.0.1:9999/ping"
curl "http://127.0.0.1:9999/api?key=Tom"
curl -X POST "http://127.0.0.1:9999/api/increment?key=counter&delta=1"
curl "http://127.0.0.1:9999/debug/stats"
```

Useful flags:

| Flag | Default | Description |
| --- | --- | --- |
| `-port` | `8001` | Peer HTTP server port. |
| `-api` | `false` | Start the public API server. |
| `-api-addr` | `0.0.0.0:9999` | API listen address. |
| `-self-addr` | `http://localhost:<port>` | Current node address used by peer routing. |
| `-peers` | local `8001,8002,8003` demo peers | Comma-separated peer addresses. |
| `-redis-addr` | `127.0.0.1:6379` | Redis backend address. |
| `-strategy` | `sharded` | Cache strategy: `sharded`, `unsharded`, `slru`, or `sharded-slru`. |
| `-shards` | `256` | Shard count for sharded mode. |
| `-cache-bytes` | `2048` | Cache capacity in bytes. |
| `-backend` | `demo` | Backend mode: `demo` uses Redis plus built-in fallback data; `synthetic` skips Redis and returns deterministic benchmark keys. |
| `-eviction-log` | `true` | Log cache eviction callbacks. Disable during high-churn benchmarks to avoid log I/O dominating tail latency. |
| `-filter` | `false` | Enable Bloom filter pre-checks. |
| `-filter-size` | `1000` | Bloom filter bitset size. |
| `-filter-hashes` | `6` | Bloom filter hash count. |
| `-filter-refresh` | `5m` | Filter rebuild interval; `<=0` disables refresh. |
| `-warmup-keys` | built-in demo keys | Comma-separated filter warmup keys. |
| `-latency-sample-rate` | `1.0` | Latency percentile sampling rate. |

## Docker and Kubernetes

Build the container:

```powershell
docker build -t cachecore:local .
```

Run the Kubernetes demo manifests:

```powershell
kubectl apply -k ./deployments/k8s
```

See `deployments/k8s/README.md` for the kind workflow, services, and
troubleshooting commands.

## Performance Tools

The `perf/` directory contains helpers for k6 load tests, Go benchmarks, pprof
capture, and wrk-based peer RPC decomposition.

Examples:

```powershell
go test ./... -run '^$' -bench . -benchmem
go test ./internal/lru -run '^$' -bench . -benchmem
go test ./internal/singleflight -run '^$' -bench . -benchmem
./perf/run-k6.ps1 -Mode mixed -VUs 120 -Duration 60s
wsl -e sh -lc 'cd /mnt/e/goCache && WRK_DURATION=20s CACHE_BYTES=65536 WRK_SHARDS=256 HOT_KEYS=64 HOT_REPEATS=16 SLRU_SCAN_BURST=128 ./perf/compare-slru-wrk.sh'
```

## Development

Run all tests:

```powershell
go test ./... -count=1
```

Build the demo server:

```powershell
go build ./cmd/cache-server
```

Regenerate protobuf code after editing `internal/groupcachepb/pb.proto`:

```powershell
protoc --go_out=. --go_opt=paths=source_relative internal/groupcachepb/pb.proto
```

## Notes

- The root package name is `cache`, even though the repository is `cachecore`.
- `internal/groupcachepb` is not part of the public API.
- The demo server seeds Redis with `Tom`, `Jack`, `Sam`, and `key0` through `key100` when Redis is reachable.
