package main

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	groupcache "goCache"
)

type benchDynamicConfig struct {
	name             string
	policy           groupcache.AutoSwitchPolicy
	switchInterval   time.Duration
	switchCooldown   time.Duration
	fallbackTTL      time.Duration
	phaseInterval    time.Duration
	warmColdModulo   uint64
	coldBurstPercent uint64
}

func disableLatencySamplingForMainBench(b *testing.B) {
	groupcache.Stats.SetLatencySamplingEnabled(false)
	b.Cleanup(func() {
		groupcache.Stats.SetLatencySamplingEnabled(true)
	})
}

func newStrategyBenchGroup(b *testing.B, strategy string, dynamicCfg *benchDynamicConfig) *groupcache.Group {
	name := fmt.Sprintf("bench-%s-%d", strategy, time.Now().UnixNano())
	opts := []groupcache.Option{}

	switch strategy {
	case "sharded":
		opts = append(opts, groupcache.WithShardedCache(256))
	case "dynamic":
		if dynamicCfg == nil {
			dynamicCfg = &benchDynamicConfig{
				policy: groupcache.AutoSwitchPolicy{
					Enable:          true,
					MissRateHigh:    0.35,
					MissRateLow:     0.10,
					HighConsecutive: 2,
					LowConsecutive:  3,
				},
				switchInterval:   10 * time.Millisecond,
				switchCooldown:   20 * time.Millisecond,
				fallbackTTL:      50 * time.Millisecond,
				phaseInterval:    40 * time.Millisecond,
				warmColdModulo:   10,
				coldBurstPercent: 8,
			}
		}
		opts = append(opts,
			groupcache.WithFallbackTTL(dynamicCfg.fallbackTTL),
			groupcache.WithSwitchCooldown(dynamicCfg.switchCooldown),
			groupcache.WithAutoSwitchByMissRate(dynamicCfg.policy),
		)
	}

	g := groupcache.NewGroup(name, 8<<20, groupcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("value:" + key), nil
	}), opts...)

	if strategy == "dynamic" {
		g.StartAutoSwitchController(dynamicCfg.switchInterval, newWindowMissRateSampler(groupcache.Stats.Snapshot))
		b.Cleanup(func() {
			g.StopAutoSwitchController()
		})
	}

	return g
}

func BenchmarkStrategyComparisonNoPopup(b *testing.B) {
	disableLatencySamplingForMainBench(b)

	cases := []struct {
		name       string
		strategy   string
		dynamicCfg *benchDynamicConfig
	}{
		{
			name:     "dynamic-conservative",
			strategy: "dynamic",
			dynamicCfg: &benchDynamicConfig{
				name: "conservative",
				policy: groupcache.AutoSwitchPolicy{
					Enable:          true,
					MissRateHigh:    0.45,
					MissRateLow:     0.08,
					HighConsecutive: 3,
					LowConsecutive:  6,
				},
				switchInterval:   12 * time.Millisecond,
				switchCooldown:   35 * time.Millisecond,
				fallbackTTL:      60 * time.Millisecond,
				phaseInterval:    50 * time.Millisecond,
				warmColdModulo:   12,
				coldBurstPercent: 7,
			},
		},
		{
			name:     "dynamic-balanced",
			strategy: "dynamic",
			dynamicCfg: &benchDynamicConfig{
				name: "balanced",
				policy: groupcache.AutoSwitchPolicy{
					Enable:          true,
					MissRateHigh:    0.35,
					MissRateLow:     0.10,
					HighConsecutive: 2,
					LowConsecutive:  3,
				},
				switchInterval:   10 * time.Millisecond,
				switchCooldown:   20 * time.Millisecond,
				fallbackTTL:      50 * time.Millisecond,
				phaseInterval:    40 * time.Millisecond,
				warmColdModulo:   10,
				coldBurstPercent: 8,
			},
		},
		{
			name:     "dynamic-aggressive",
			strategy: "dynamic",
			dynamicCfg: &benchDynamicConfig{
				name: "aggressive",
				policy: groupcache.AutoSwitchPolicy{
					Enable:          true,
					MissRateHigh:    0.25,
					MissRateLow:     0.15,
					HighConsecutive: 1,
					LowConsecutive:  1,
				},
				switchInterval:   8 * time.Millisecond,
				switchCooldown:   8 * time.Millisecond,
				fallbackTTL:      30 * time.Millisecond,
				phaseInterval:    30 * time.Millisecond,
				warmColdModulo:   8,
				coldBurstPercent: 9,
			},
		},
		{name: "static-sharded", strategy: "sharded"},
		{name: "static-unsharded", strategy: "unsharded"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			g := newStrategyBenchGroup(b, tc.strategy, tc.dynamicCfg)
			if _, err := g.Get(context.Background(), "hot-key"); err != nil {
				b.Fatalf("warmup get failed: %v", err)
			}

			before := groupcache.Stats.Snapshot()
			var seq uint64
			var phase uint32
			stopPhase := make(chan struct{})
			go func() {
				phaseInterval := 40 * time.Millisecond
				if tc.dynamicCfg != nil && tc.dynamicCfg.phaseInterval > 0 {
					phaseInterval = tc.dynamicCfg.phaseInterval
				}
				ticker := time.NewTicker(phaseInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						atomic.AddUint32(&phase, 1)
					case <-stopPhase:
						return
					}
				}
			}()
			b.Cleanup(func() {
				close(stopPhase)
			})

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := atomic.AddUint64(&seq, 1)
					curPhase := atomic.LoadUint32(&phase) % 4
					warmColdModulo := uint64(10)
					coldBurstPercent := uint64(8)
					if tc.dynamicCfg != nil {
						if tc.dynamicCfg.warmColdModulo > 0 {
							warmColdModulo = tc.dynamicCfg.warmColdModulo
						}
						if tc.dynamicCfg.coldBurstPercent > 0 {
							coldBurstPercent = tc.dynamicCfg.coldBurstPercent
						}
					}

					key := "hot-key"
					switch curPhase {
					case 0, 1:
						if n%warmColdModulo == 0 {
							key = "warm-cold-" + strconv.FormatUint(n%64, 10)
						}
					default:
						if n%10 < coldBurstPercent {
							key = "cold-" + strconv.FormatUint(n, 10)
						}
					}

					if _, err := g.Get(context.Background(), key); err != nil {
						b.Fatalf("get failed: %v", err)
					}
				}
			})
			b.StopTimer()

			after := groupcache.Stats.Snapshot()
			switchToSharded := float64(after.SwitchToSharded - before.SwitchToSharded)
			switchToUnsharded := float64(after.SwitchToUnsharded - before.SwitchToUnsharded)
			switches := switchToSharded + switchToUnsharded
			b.ReportMetric(switchToSharded, "to_sharded")
			b.ReportMetric(switchToUnsharded, "to_unsharded")
			b.ReportMetric(switches, "switches")
		})
	}
}
