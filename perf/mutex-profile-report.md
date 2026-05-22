# Mutex Profile 分析报告

日期：2026-04-16

## 分析范围

本次分析覆盖了两条路径：

1. 基于 `go test -bench -mutexprofile` 的离线 Benchmark 锁竞争分析
2. 基于 `http://127.0.0.1:9999/debug/pprof/mutex` 的在线 API 锁竞争分析

其中，离线 Benchmark 路径已成功完成。
在线路径由于 `9999` 端口已被一个会接受连接但不返回 HTTP 响应的进程占用，未能完成有效采集。

## 执行命令

### 1. 热点 Key Benchmark

```powershell
go test . -run '^$' -bench BenchmarkGroupGetParallelHotKey -benchmem -mutexprofile test-logs\bench-hot.mutex.out
go tool pprof -top .\goCache.test.exe test-logs\bench-hot.mutex.out
go tool pprof -sample_index=contentions -top .\goCache.test.exe test-logs\bench-hot.mutex.out
```

Benchmark 结果：

```text
BenchmarkGroupGetParallelHotKey-32    	 1000000	      3447 ns/op	       0 B/op	       0 allocs/op
```

### 2. 策略对比 Benchmark

```powershell
$env:GOCACHE='E:\goCache\.gocache'
go test . -run '^$' -bench BenchmarkGroupGetStrategyComparisonParallel -benchmem -mutexprofile test-logs\strategy.mutex.out
go tool pprof -top .\goCache.test.exe test-logs\strategy.mutex.out
go tool pprof -sample_index=contentions -top .\goCache.test.exe test-logs\strategy.mutex.out
```

Benchmark 结果：

```text
BenchmarkGroupGetStrategyComparisonParallel/dynamic-auto-switch-32         	  656637	      4584 ns/op	     158 B/op	       3 allocs/op
BenchmarkGroupGetStrategyComparisonParallel/static-sharded-32              	  417042	      2779 ns/op	       9 B/op	       0 allocs/op
BenchmarkGroupGetStrategyComparisonParallel/static-unsharded-32            	  400591	      2802 ns/op	      10 B/op	       0 allocs/op
```

### 3. 在线 API 路径尝试

尝试流程：

```powershell
go build -o perf\server-profile.exe .\main
perf\server-profile.exe -port=8001 -api=true -mutex-profile-fraction=1 -block-profile-rate=1
Invoke-WebRequest http://127.0.0.1:9999/debug/stats
Invoke-WebRequest http://127.0.0.1:9999/debug/pprof/mutex?debug=0
```

观察到的问题：

- `0.0.0.0:9999` 已有进程监听
- 请求 `/debug/stats` 和 `/debug/pprof/` 都会超时
- 新起的 API 服务报错：

```text
listen tcp 0.0.0.0:9999: bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.
```

相关 `netstat` 输出：

```text
TCP    0.0.0.0:9999     0.0.0.0:0     LISTENING     43316
TCP    [::]:9999        [::]:0        LISTENING     43316
```

## 初始发现

### 1. 初始 profile 的最大热点并不是缓存锁本身

在第一轮 profile 中，最大的互斥锁热点实际上是 [`stats.go`](/e:/goCache/stats.go) 里的 `latencyWindow.observe()`。

其上游路径主要包括：

- `goCache.(*Metrics).RecordGroupGetLatency`
- `goCache.(*Metrics).RecordCacheGetLatency`

这说明最初的 mutex profile 被统计采样本身的锁竞争严重干扰。

### 2. 热点 Key 场景下，统计锁掩盖了缓存锁信号

从 `perf/mutex-bench-hot-top.txt` 可见：

- 总 mutex delay：`93.56s`
- `goCache.(*latencyWindow).observe`：`93.53s cum`
- `goCache.(*cache).get`：`35.68s cum`
- `goCache.(*Metrics).RecordGroupGetLatency`：`57.87s cum`
- `goCache.(*Metrics).RecordCacheGetLatency`：`35.66s cum`

从 `perf/mutex-bench-hot-contentions.txt` 可见：

- 总 contentions：`619,693`
- `goCache.(*latencyWindow).observe`：`619,092 cum`
- `goCache.(*cache).get`：`300,703 cum`

结论：

- 初始 profile 确实包含缓存路径锁竞争
- 但统计采样路径带来的竞争足够大，导致缓存主锁不是最显眼的热点

### 3. 初始策略对比中，sharded 与 unsharded 差异不明显

第一轮 benchmark 数据：

- `static-sharded`：`2779 ns/op`
- `static-unsharded`：`2802 ns/op`
- `dynamic-auto-switch`：`4584 ns/op`

结论：

- 在有统计噪声时，`sharded` 和 `unsharded` 的性能差异不明显
- `dynamic-auto-switch` 更慢，主要是额外协调与分配开销造成
- 这一轮不能直接说明“缓存分片收益不大”，因为信号被统计锁干扰了

### 4. 在线 `pprof/mutex` 超时的直接原因是 `9999` 端口上的异常监听进程

这也解释了此前命令：

```text
go tool pprof http://127.0.0.1:9999/debug/pprof/mutex
```

会报：

```text
net/http: timeout awaiting response headers
```

问题不在 `pprof` 格式本身，而在于目标 HTTP 服务没有返回响应头。

## 去噪后的重跑

为了去掉统计采样噪声，本次在 Benchmark 执行期间临时关闭了 `latencyWindow.observe()` 这一类延迟样本窗口记录，但保留了总次数、总耗时等原子计数。

代码调整如下：

- [`stats.go`](/e:/goCache/stats.go)：新增 `Metrics.SetLatencySamplingEnabled(bool)`
- [`groupcache_bench_test.go`](/e:/goCache/groupcache_bench_test.go)：在各 Benchmark 中通过 `b.Cleanup(...)` 临时关闭采样

### 1. 去噪后的热点 Key Benchmark

执行命令：

```powershell
$env:GOCACHE='E:\goCache\.gocache'
go test . -run '^$' -bench BenchmarkGroupGetParallelHotKey -benchmem -mutexprofile test-logs\bench-hot-noise-removed.mutex.out
```

结果：

```text
BenchmarkGroupGetParallelHotKey-32    	 5720313	       212.6 ns/op	       0 B/op	       0 allocs/op
```

与去噪前相比：

- 去噪前：`3447 ns/op`
- 去噪后：`212.6 ns/op`

结论：

- 原始 benchmark 明显被统计采样锁拖慢
- 去掉噪声后，热点路径性能大幅提升
- 同时 mutex profile 也开始暴露出真正的缓存锁路径

从 `perf/mutex-bench-hot-noise-removed-top.txt` 可见：

- 总 mutex delay：`42.40s`
- `goCache.(*cache).get`：`42.14s cum`
- `goCache.(*latencyWindow).observe`：已不再是主要热点

从 `perf/mutex-bench-hot-noise-removed-contentions.txt` 可见：

- 总 contentions：`523,984`
- `goCache.(*cache).get`：`496,339 cum`

结论：

- 去噪后，锁竞争主要集中在缓存读路径本身
- 这才是分析缓存锁竞争时更可信的 profile

### 2. 去噪后的策略对比 Benchmark

执行命令：

```powershell
$env:GOCACHE='E:\goCache\.gocache'
go test . -run '^$' -bench BenchmarkGroupGetStrategyComparisonParallel -benchtime=1s -benchmem -mutexprofile test-logs\strategy-noise-removed.mutex.out
```

结果：

```text
BenchmarkGroupGetStrategyComparisonParallel/dynamic-auto-switch-32         	 1747578	       763.0 ns/op	     158 B/op	       3 allocs/op
BenchmarkGroupGetStrategyComparisonParallel/static-sharded-32              	 4418643	       340.9 ns/op	       5 B/op	       0 allocs/op
BenchmarkGroupGetStrategyComparisonParallel/static-unsharded-32            	 2023350	       561.1 ns/op	       5 B/op	       0 allocs/op
```

结论：

- 去噪后，`static-sharded` 明显快于 `static-unsharded`
- 分片缓存对降低锁竞争、提升吞吐是有效的
- `dynamic-auto-switch` 依旧更慢，说明动态策略的协调成本仍然存在

从 `perf/mutex-strategy-noise-removed-top.txt` 可见：

- `goCache.(*cache).get`：`102.28s cum`
- `goCache.(*ShardedCache).get`：`36.34s cum`
- `goCache.(*cache).add`：`11.19s cum`
- `goCache/singleflight.(*Group).Do`：`33.20s cum`

从 `perf/mutex-strategy-noise-removed-contentions.txt` 可见：

- `goCache.(*cache).get`：`1,350,979 cum`
- `goCache.(*ShardedCache).get`：`412,414 cum`
- `goCache/singleflight.(*Group).Do`：`312,627 cum`
- `goCache.(*cache).add`：`98,473 cum`

结论：

- 去噪后，profile 中主要保留下来的是真实缓存路径和 `singleflight` 协调路径
- 当前主要争用点来自缓存读、miss 后写入，以及单飞去重

## 派生指标计算

以下计算都基于去噪后的 profile。

### 1. 热点 Key 场景

原始数据：

- 总操作数：`5,720,313`
- 估算 benchmark 墙钟时间：`5,720,313 * 212.6ns = 1.216s`
- mutex delay：`42.40s`
- contentions：`523,984`
- 并发度估计：`32`

派生指标：

- 平均每次竞争等待时间：

```text
42.40s / 523,984 = 80.9us
```

- 每个操作平均带来的累计阻塞时间：

```text
42.40s / 5,720,313 = 7.41us/op
```

- 每个操作的竞争事件数：

```text
523,984 / 5,720,313 = 0.0916
```

- 近似累计阻塞时间占比：

```text
42.40s / (1.216s * 32) = 1.089 ~= 108.9%
```

说明：

- 平均每次锁竞争的代价约为 `80.9us`
- 每个请求平均会产生 `7.41us` 的累计锁等待损耗
- 大约 `9.16%` 的操作会对应到一次采样到的竞争事件
- 这里的 `108.9%` 不是“CPU 使用率超过 100%”，而是并发线程累计等待时间已经接近 `1.09` 倍完整并发预算，说明串行化效应明显

### 2. 策略对比场景

原始数据：

- Dynamic：`1,747,578` 次，`763.0ns/op`
- Sharded：`4,418,643` 次，`340.9ns/op`
- Unsharded：`2,023,350` 次，`561.1ns/op`
- 总操作数：`8,189,571`
- 估算 benchmark 墙钟时间：

```text
1,747,578*763.0ns + 4,418,643*340.9ns + 2,023,350*561.1ns = 3.975s
```

- mutex delay：`139.02s`
- contentions：`1,762,277`
- 并发度估计：`32`

派生指标：

- 平均每次竞争等待时间：

```text
139.02s / 1,762,277 = 78.9us
```

- 每个操作平均带来的累计阻塞时间：

```text
139.02s / 8,189,571 = 16.98us/op
```

- 每个操作的竞争事件数：

```text
1,762,277 / 8,189,571 = 0.215
```

- 近似累计阻塞时间占比：

```text
139.02s / (3.975s * 32) = 1.093 ~= 109.3%
```

说明：

- 平均每次锁竞争等待约 `79us`
- 每个操作平均会带来 `16.98us` 的累计阻塞时间
- 该场景下的竞争事件密度更高，约为 `0.215` 次/操作
- `109.3%` 同样表示线程累计等待时间已经超过一个完整并发窗口，锁串行化较明显

### 3. 策略场景下的函数级平均成本

基于去噪后的策略 profile：

- `goCache.(*cache).get`

```text
102.28s / 1,350,979 = 75.7us/次竞争
```

- `goCache.(*ShardedCache).get`

```text
36.34s / 412,414 = 88.1us/次竞争
```

- `goCache/singleflight.(*Group).Do`

```text
33.20s / 312,627 = 106.2us/次竞争
```

- `goCache.(*cache).add`

```text
11.19s / 98,473 = 113.6us/次竞争
```

说明：

- 最频繁的争用来源仍然是缓存读路径
- `singleflight` 和 `cache.add` 出现次数更少，但单次竞争成本更高
- 这些值是 cumulative 统计，适合做方向性分析，不适合简单直接相加

## 输出文件

本次生成的主要结果文件包括：

- `perf/mutex-bench-hot-top.txt`
- `perf/mutex-bench-hot-contentions.txt`
- `perf/mutex-bench-hot-noise-removed-top.txt`
- `perf/mutex-bench-hot-noise-removed-contentions.txt`
- `perf/mutex-strategy-top.txt`
- `perf/mutex-strategy-contentions.txt`
- `perf/mutex-strategy-noise-removed-top.txt`
- `perf/mutex-strategy-noise-removed-contentions.txt`
- `perf/server-profile.err.log`
- `perf/server-profile.out.log`
- `perf/server-profile.exe`

## 建议

1. 当目标是分析缓存锁竞争时，应优先使用去噪后的 benchmark 路径。
2. 正常服务运行中保留延迟采样，但做 mutex 微基准时建议暂时关闭采样窗口记录。
3. 清理掉占用 `9999` 的异常进程后，再重新执行在线 profile：

```powershell
go run .\main -api=true -port=8001 -mutex-profile-fraction=1
go tool pprof http://127.0.0.1:9999/debug/pprof/mutex
```

4. 解读 mutex profile 时，可按以下方式理解：

```text
delay = 累计锁等待时间
contentions = 锁竞争事件次数
avg_wait_per_contention = delay / contentions
```

需要注意的是，mutex profile 本身并不会直接给出 P99 锁等待时间。

## 2026-04-18 Update

The historical `dynamic-auto-switch` numbers in this report were collected before the dynamic policy was changed to use window-based miss-rate sampling. They should be treated as a record of the old strategy rather than the current default behavior.

Current code changes:

1. Dynamic switching now uses window delta miss rate instead of lifetime cumulative miss rate.
2. The default dynamic parameters in `main/main.go` were updated to a more conservative profile:

- `switch-interval=3s`
- `miss-high=0.45`
- `miss-low=0.08`
- `high-consecutive=3`
- `low-consecutive=6`
- `fallback-ttl=120s`
- `switch-cooldown=90s`

A no-popup benchmark was also added for local verification:

```powershell
go test .\main -run '^$' -bench BenchmarkStrategyComparisonNoPopup -benchmem -benchtime=3s
```

One local run on 2026-04-18 produced:

```text
BenchmarkStrategyComparisonNoPopup/dynamic-conservative-32   584.9 ns/op   44 switches
BenchmarkStrategyComparisonNoPopup/dynamic-balanced-32       593.2 ns/op   40 switches
BenchmarkStrategyComparisonNoPopup/dynamic-aggressive-32     658.8 ns/op   62 switches
BenchmarkStrategyComparisonNoPopup/static-sharded-32         554.2 ns/op    0 switches
BenchmarkStrategyComparisonNoPopup/static-unsharded-32       714.7 ns/op    0 switches
```

Interpretation:

- `static-sharded` remains the strongest baseline for the current mixed workload.
- Conservative dynamic thresholds already outperform `static-unsharded`.
- Aggressive thresholds regress mainly because they switch too often.

Recommended default going forward:

- Default runtime strategy should be `sharded`.
- `unsharded` is best kept as a baseline/control mode.
- `dynamic` is still useful for experiments, but it should be treated as experimental rather than the default optimization path.
