// heatmapd 演示：在本进程内模拟节点采样上报，
// 聚合为热力图并周期性打印集群级视图。
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go/internal/heatmap"
)

func main() {
	var (
		dataDir = flag.String("data-dir", "", "持久化目录，空为纯内存")
		window  = flag.Duration("window", 10*time.Second, "窗口长度")
		step    = flag.Duration("step", 10*time.Second, "滑动步长")
		nodes   = flag.Int("nodes", 8, "模拟节点数")
		rate    = flag.Int("rate", 200, "每秒采样数")
	)
	flag.Parse()

	cfg := heatmap.Config{
		WindowSize:      *window,
		SlideStep:       *step,
		AllowedLateness: 5 * time.Second,
		RevisionHorizon: 30 * time.Second,
		QueueCapacity:   4096,
		DataDir:         *dataDir,
		SnapshotEvery:   2 * time.Second,
	}
	svc, err := heatmap.NewService(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 模拟采样源：内存管道直推，含乱序、迟到与重复。
	go simulate(ctx, svc, *nodes, *rate)

	// 周期性打印集群级热力图。
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("shutting down...")
			if err := svc.Close(); err != nil {
				fmt.Fprintln(os.Stderr, "close:", err)
			}
			return
		case <-ticker.C:
			printHeatmap(svc)
		}
	}
}

// simulate 生成模拟采样：正常流 + 5% 乱序迟到 + 2% 重复。
func simulate(ctx context.Context, svc *heatmap.Service, nodes, rate int) {
	rng := rand.New(rand.NewSource(42))
	pools := []string{"pool-a", "pool-b"}
	resources := []heatmap.ResourceType{heatmap.ResourceCPU, heatmap.ResourceMemory}
	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			seq++
			ts := time.Now()
			if rng.Float64() < 0.05 { // 迟到/乱序
				ts = ts.Add(-time.Duration(rng.Intn(8)) * time.Second)
			}
			node := fmt.Sprintf("node-%d", rng.Intn(nodes))
			smp := heatmap.Sample{
				SampleID:  fmt.Sprintf("sim-%d", seq),
				NodeID:    node,
				Pool:      pools[rng.Intn(len(pools))],
				Resource:  resources[rng.Intn(len(resources))],
				Value:     rng.Float64() * 100,
				Timestamp: ts,
			}
			if err := svc.Ingest(ctx, smp); err != nil {
				continue // 背压拒绝：采样方可观测后重试或丢弃
			}
			if rng.Float64() < 0.02 { // 重复上报
				svc.Ingest(ctx, smp)
			}
		}
	}
}

func printHeatmap(svc *heatmap.Service) {
	rows := svc.Query(heatmap.Query{Resource: heatmap.ResourceCPU}, heatmap.GroupByPool)
	m := svc.Metrics()
	fmt.Printf("\n== CPU heatmap by pool (watermark=%s) ==\n",
		time.Unix(0, svc.Snapshot().Watermark()).UTC().Format("15:04:05"))
	for _, r := range rows {
		fmt.Printf("  window %s  %-8s count=%-5d avg=%6.2f min=%6.2f max=%6.2f\n",
			r.Window.Start.UTC().Format("15:04:05"), r.Group, r.Count, r.Avg(), r.Min, r.Max)
	}
	fmt.Printf("  metrics: ingested=%d dup=%d tooLate=%d revisions=%d backpressure=%d persistErr=%d\n",
		m.Ingested, m.Duplicates, m.TooLate, m.Revisions, m.Rejected, m.PersistErrors)
}
