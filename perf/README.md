# 性能测试工具包

这个目录提供了一套实用的性能测试流程：

1. 使用 k6 做 API 压测。
2. 使用 `go test -bench` 做组件级基准测试。
3. 使用 `pprof` 做 CPU 和内存热点分析。

## 1) API 压测（k6）

请先启动你的缓存节点和 API 服务。

安装 k6（Windows 下的 Go 兜底方案）：

```powershell
go install go.k6.io/k6@latest
```

示例：

```powershell
./perf/run-k6.ps1 -Mode mixed -VUs 120 -Duration 60s
```

严格 SLO 示例：

```powershell
./perf/run-k6.ps1 -Mode mixed -VUs 120 -Duration 60s -ThresholdP95Ms 10 -ThresholdP99Ms 30 -ThresholdErrorRate 0.001
```

常用参数：

- `-Mode hot`：只压热点 key（`Tom`）
- `-Mode mixed`：70% 热点 key + 30% 已知有效 key（由 `-ValidKeys` 指定，默认 `Tom`）
- `-MixedIncludeSeeded`：在 mixed 模式下使用预置数字 key（是否会出现 404/500 取决于过滤器策略）
- `-ValidKeys`：mixed 模式使用的 key 列表，逗号分隔，例如 `Tom,Jack`
- `-VUs`：虚拟用户数
- `-Duration`：压测时长，例如 `30s`、`2m`
- `-ThresholdP95Ms`：p95 延迟预算（毫秒，默认 10）
- `-ThresholdP99Ms`：p99 延迟预算（毫秒，默认 30）
- `-ThresholdErrorRate`：错误率预算（默认 0.001）
- `-FailOnThreshold`：当阈值未达标时返回非 0 退出码（推荐在 CI 中使用）

输出文件会保存到 `test-logs/k6`。

`run-k6.ps1` 会在 k6 执行后自动运行 `analyze-k6-summary.ps1`，并输出基于预算的 PASS/FAIL 结果。
默认情况下阈值失败不会以非 0 退出；如需严格 CI 行为，请使用 `-FailOnThreshold`。

可选的 Vegeta 脚本仍可使用：`./perf/run-vegeta.ps1`。

## 2) 基准测试（Benchmark）

运行所有基准测试：

```powershell
go test ./... -run '^$' -bench . -benchmem
```

按包运行：

```powershell
go test . -run '^$' -bench BenchmarkGroup -benchmem
go test ./internal/lru -run '^$' -bench . -benchmem
go test ./internal/singleflight -run '^$' -bench . -benchmem
go test ./bloomfilter -run '^$' -bench . -benchmem
```

## 3) pprof 热点分析

采集 CPU + Heap profile（API 服务必须使用 `-api=true` 启动）：

```powershell
./perf/run-pprof.ps1 -ProfileSeconds 30
```

查看 profile：

```powershell
go tool pprof -http=:0 .\\server.exe test-logs\\pprof\\cpu-*.pb.gz
go tool pprof -top .\\server.exe test-logs\\pprof\\heap-*.pb.gz
```

说明：`cmd/cache-server/main.go` 已导入 `net/http/pprof`，因此 API 服务会暴露 `/debug/pprof/*`。

## 4) Static Strategy Notes

No-popup static strategy comparison benchmark:

```powershell
go test .\main -run '^$' -bench BenchmarkStrategyComparisonNoPopup -benchmem -benchtime=3s
```

Operational recommendation:

- Use `-strategy=sharded` as the default production choice.
- Use `-strategy=unsharded` only for explicit baseline comparison.
