package heatmap

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 背压：队列满时确定性拒绝，且可观测。
func TestServiceBackpressure(t *testing.T) {
	cfg := testConfig()
	cfg.QueueCapacity = 2
	store, _ := openPersistStore("")
	s := &Service{
		cfg:   cfg,
		queue: make(chan Sample, cfg.QueueCapacity),
		agg:   newAggregator(cfg),
		store: store,
	}
	// 不启动 run goroutine，队列只进不出。
	for i := 0; i < 2; i++ {
		if err := s.Ingest(context.Background(), mkSample(fmt.Sprintf("s%d", i), "n0", int64(i), 1)); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	err := s.Ingest(context.Background(), mkSample("s2", "n0", 2, 1))
	if err != ErrBackpressure {
		t.Fatalf("want ErrBackpressure, got %v", err)
	}
	if got := s.Metrics().Rejected; got != 1 {
		t.Fatalf("Rejected = %d, want 1", got)
	}
}

// 取消的 ctx 不产生任何写入。
func TestServiceIngestCanceled(t *testing.T) {
	s, err := NewService(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Ingest(ctx, mkSample("x", "n0", 1, 1)); err == nil {
		t.Fatal("want error for canceled ctx")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(s.Snapshot().Cells()) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("canceled ingest leaked into state")
}

// 并发查询与聚合：任一时刻查询结果内部自洽（集群级 == 节点级之和）。
func TestServiceConcurrentQueryConsistency(t *testing.T) {
	cfg := testConfig()
	cfg.WindowSize = time.Hour // 单窗口，便于校验
	cfg.SlideStep = time.Hour
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				smp := mkSample(fmt.Sprintf("w%d-%d", w, i), fmt.Sprintf("n%d", i%5), int64(i%60), float64(i))
				s.Ingest(context.Background(), smp)
			}
		}(w)
	}

	for i := 0; i < 200; i++ {
		snap := s.Snapshot()
		q := Query{Resource: ResourceCPU}
		nodeRows := QuerySnapshot(snap, q, GroupByNode)
		clusterRows := QuerySnapshot(snap, q, GroupByCluster)
		var nodeCount, clusterCount int64
		var nodeSum, clusterSum float64
		for _, r := range nodeRows {
			nodeCount += r.Count
			nodeSum += r.Sum
		}
		for _, r := range clusterRows {
			clusterCount += r.Count
			clusterSum += r.Sum
		}
		if nodeCount != clusterCount || nodeSum != clusterSum {
			t.Fatalf("mixed state: node(%d,%v) cluster(%d,%v)", nodeCount, nodeSum, clusterCount, clusterSum)
		}
	}
	close(stop)
	wg.Wait()
}

// 重启恢复：已封口窗口与修订完整还原，未封口进度不丢失、不重复计入。
func TestServiceRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.DataDir = dir
	cfg.SnapshotEvery = 50 * time.Millisecond

	s1, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	samples := []Sample{
		mkSample("a", "n0", 5, 1.0),
		mkSample("b", "n0", 10, 2.0),
		mkSample("c", "n0", 100, 3.0), // 推进水位，封口 [0,60)
		mkSample("d", "n0", 20, 4.0),  // 迟到补算 -> 修订 v2
		mkSample("e", "n1", 110, 5.0), // 未封口窗口 [60,120)
	}
	for _, smp := range samples {
		if err := s1.Ingest(context.Background(), smp); err != nil {
			t.Fatal(err)
		}
	}
	// 等待处理完成
	waitFor(t, func() bool { return s1.Metrics().Ingested == 5 })
	s1.Checkpoint()
	before := s1.Snapshot()
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启
	s2, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	after := s2.Snapshot()

	bc, ac := before.Cells(), after.Cells()
	if len(bc) != len(ac) {
		t.Fatalf("cell count %d != %d\nbefore=%+v\nafter=%+v", len(bc), len(ac), bc, ac)
	}
	for i := range bc {
		if bc[i] != ac[i] {
			t.Fatalf("cell %d mismatch:\nbefore %+v\nafter  %+v", i, bc[i], ac[i])
		}
	}
	br, ar := before.Revisions(), after.Revisions()
	if len(br) != len(ar) {
		t.Fatalf("revisions %d != %d", len(br), len(ar))
	}
	for i := range br {
		if br[i].Version != ar[i].Version || br[i].Window != ar[i].Window || br[i].NodeID != ar[i].NodeID {
			t.Fatalf("revision %d mismatch: %+v vs %+v", i, br[i], ar[i])
		}
	}

	// 恢复后继续摄入：重复样本被去重，不重复计入
	if err := s2.Ingest(context.Background(), mkSample("a", "n0", 5, 1.0)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return s2.Metrics().Duplicates == 1 })
	for _, c := range s2.Snapshot().Cells() {
		if c.Window.Start == ts(0) && c.NodeID == "n0" && c.Count != 3 {
			t.Fatalf("duplicate after recovery counted twice: %+v", c)
		}
	}
}

// 崩溃模拟：不做最终 checkpoint 直接丢弃（Close 前杀掉），
// 已接受的采样仍应从 WAL 恢复。
func TestServiceCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.DataDir = dir
	cfg.SnapshotEvery = time.Hour // 禁止周期 checkpoint

	s1, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		s1.Ingest(context.Background(), mkSample(fmt.Sprintf("s%d", i), "n0", int64(i), float64(i)))
	}
	// 等快照发布（发生在 WAL 落盘之后），确保处理彻底完成
	waitFor(t, func() bool {
		var n int64
		for _, c := range s1.Snapshot().Cells() {
			n += c.Count
		}
		return n == 10
	})
	// 模拟崩溃：不调用 Close（run goroutine 泄漏在测试中可接受，进程结束即清理）
	s1.store.close() // 仅关闭文件句柄，模拟落盘已同步

	s2, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var total int64
	for _, c := range s2.Snapshot().Cells() {
		total += c.Count
	}
	if total != 10 {
		t.Fatalf("recovered count = %d, want 10", total)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
