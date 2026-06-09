# cachecore

[English](README.md) | [简体中文](README.zh-CN.md)

`cachecore` 是一个受 groupcache 启发的 Go 分布式缓存库和示例服务端。它提供本地 LRU 缓存、基于 HTTP 的节点间取值、singleflight 请求合并、可选布隆过滤器预检查、负缓存、TTL 抖动、stale-while-revalidate 降级，以及轻量级运行时指标。

公开 Go 包为：

```go
import cache "github.com/Thadopz/cachecore"
```

## 功能特性

- 基于内存的 LRU 缓存，支持字节容量限制、TTL 过期和淘汰回调。
- 使用一致性哈希进行分布式 peer 路由，并通过 protobuf-over-HTTP 传输数据。
- 使用 singleflight 合并同一个 key 的并发 miss 加载。
- 支持可选的分片缓存模式，降低高并发场景下的锁竞争。
- 支持通过 `WithFilter` 和公开的 `bloomfilter` 包启用布隆过滤器。
- 支持对 `ErrNotFound` 进行负缓存。
- 支持在等待 in-flight load 超时时使用 stale-while-revalidate 降级。
- 示例服务端通过 `/debug/stats` 暴露计数器和延迟采样指标。
- 提供 Redis 回源示例服务端、Dockerfile、Kubernetes 清单和性能测试工具。

## 项目结构

```text
.
|-- cmd/cache-server/        # Redis 回源示例服务端和 API
|-- bloomfilter/             # 可传给 WithFilter 的公开布隆过滤器实现
|-- deployments/k8s/         # Kubernetes 部署清单
|-- internal/consistenthash/ # 内部 peer 哈希环
|-- internal/groupcachepb/   # 内部 protobuf 传输类型
|-- internal/lru/            # 内部 LRU 缓存
|-- internal/singleflight/   # 内部请求合并实现
|-- perf/                    # benchmark、k6、pprof 和 wrk 辅助工具
`-- *.go                     # 公开 cache 包
```

## 作为库使用

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

## 分布式 Peer

当多个缓存节点需要按 key 路由到 owner 节点时，使用 `HTTPPool`：

```go
pool := cache.NewHTTPPool("http://localhost:8001")
pool.Set(
	"http://localhost:8001",
	"http://localhost:8002",
	"http://localhost:8003",
)
group.RegisterPeers(pool)
```

然后用 `http.ListenAndServe` 挂载这个 pool 即可。protobuf peer 协议保留在 `internal/groupcachepb` 内部，不属于公开 API。

## 示例服务端

构建服务端：

```powershell
go build ./cmd/cache-server
```

启动一个本地节点并开启 API：

```powershell
go run ./cmd/cache-server -port=8001 -api=true -api-addr=0.0.0.0:9999
```

常用接口：

```powershell
curl "http://127.0.0.1:9999/ping"
curl "http://127.0.0.1:9999/api?key=Tom"
curl -X POST "http://127.0.0.1:9999/api/increment?key=counter&delta=1"
curl "http://127.0.0.1:9999/debug/stats"
```

常用参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-port` | `8001` | Peer HTTP 服务端口。 |
| `-api` | `false` | 是否启动公开 API 服务。 |
| `-api-addr` | `0.0.0.0:9999` | API 监听地址。 |
| `-self-addr` | `http://localhost:<port>` | 当前节点在 peer 路由中的地址。 |
| `-peers` | 本地 `8001,8002,8003` demo peers | 逗号分隔的 peer 地址。 |
| `-redis-addr` | `127.0.0.1:6379` | Redis 后端地址。 |
| `-strategy` | `sharded` | 缓存策略：`sharded` 或 `unsharded`。 |
| `-shards` | `256` | 分片模式下的分片数量。 |
| `-filter` | `false` | 启用布隆过滤器预检查。 |
| `-filter-size` | `1000` | 布隆过滤器 bitset 大小。 |
| `-filter-hashes` | `6` | 布隆过滤器哈希函数数量。 |
| `-filter-refresh` | `5m` | 过滤器重建间隔；`<=0` 表示禁用刷新。 |
| `-warmup-keys` | 内置 demo keys | 逗号分隔的过滤器预热 key。 |
| `-latency-sample-rate` | `1.0` | 延迟百分位采样率。 |

## Docker 和 Kubernetes

构建镜像：

```powershell
docker build -t cachecore:local .
```

部署 Kubernetes demo 清单：

```powershell
kubectl apply -k ./deployments/k8s
```

kind 工作流、Service 和排查命令见 `deployments/k8s/README.md`。

## 性能工具

`perf/` 目录包含 k6 压测、Go benchmark、pprof 采集和基于 wrk 的 peer RPC 拆解工具。

示例：

```powershell
go test ./... -run '^$' -bench . -benchmem
go test ./internal/lru -run '^$' -bench . -benchmem
go test ./internal/singleflight -run '^$' -bench . -benchmem
./perf/run-k6.ps1 -Mode mixed -VUs 120 -Duration 60s
```

## 开发

运行全部测试：

```powershell
go test ./... -count=1
```

构建示例服务端：

```powershell
go build ./cmd/cache-server
```

修改 `internal/groupcachepb/pb.proto` 后重新生成 protobuf 代码：

```powershell
protoc --go_out=. --go_opt=paths=source_relative internal/groupcachepb/pb.proto
```

## 说明

- 根包名是 `cache`，即使仓库名是 `cachecore`。
- `internal/groupcachepb` 不属于公开 API。
- Redis 可用时，示例服务端会预置 `Tom`、`Jack`、`Sam` 以及 `key0` 到 `key100`。
