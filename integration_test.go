package heatmap_test

import (
	"context"
	"testing"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func TestIntegration_FullPipeline(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 5 * time.Second
	cfg.WindowSize = time.Minute
	cfg.WindowSlide = time.Minute
	svc := newTestService(t, cfg)

	srcCfg := heatmap.SourceConfig{
		NumNodes:      5,
		NumPools:      2,
		Resources:     heatmap.AllResources(),
		RatePerSec:    1000,
		OutofOrderPct: 0.2,
		LatePct:       0.1,
		DuplicatePct:  0.05,
		WindowSize:    cfg.WindowSize,
		Seed:          42,
	}
	src := heatmap.NewSource(srcCfg, svc)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := src.Run(ctx); err != nil && err != context.DeadlineExceeded {
		t.Fatal(err)
	}
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	view := svc.View()
	m := svc.Metrics()
	t.Logf("metrics: %+v", m)
	t.Logf("windows: %d", len(view.WindowStarts()))

	// Verify we have windows.
	if len(view.WindowStarts()) == 0 {
		t.Fatal("no windows produced")
	}

	// Verify self-consistency: total count from heatmap equals sum of pool queries.
	var totalCount int64
	for _, ws := range view.WindowStarts() {
		cells := view.Heatmap(time.Unix(0, ws), time.Unix(0, ws).Add(cfg.WindowSize))
		for _, c := range cells {
			totalCount += c.Count
		}
	}

	var poolCount int64
	for pool := 0; pool < srcCfg.NumPools; pool++ {
		poolID := "pool-" + string(rune('0'+pool))
		for _, ws := range view.WindowStarts() {
			cells := view.QueryByPool(poolID, time.Unix(0, ws), time.Unix(0, ws).Add(cfg.WindowSize))
			for _, c := range cells {
				poolCount += c.Count
			}
		}
	}
	if poolCount != totalCount {
		t.Fatalf("pool count %d != total count %d", poolCount, totalCount)
	}

	// Verify duplicates were detected.
	if m.Duplicates == 0 {
		t.Fatal("expected duplicates to be detected")
	}

	// Verify late revisions were recorded.
	if m.LateRevisions == 0 {
		t.Fatal("expected late revisions")
	}

	// Verify no dropped samples (block policy).
	if m.SamplesDropped != 0 {
		t.Fatalf("unexpected drops: %d", m.SamplesDropped)
	}
}

func TestIntegration_RevisionConsistency(t *testing.T) {
	// Verify that revised cells equal full recomputation from all raw samples.
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	// Initial samples.
	initial := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(20*time.Second), 2),
	}
	flushAndWait(t, svc, initial)
	flushAndWait(t, svc, []heatmap.Sample{sealSample(base)})

	// Late revisions.
	late := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 30, base.Add(30*time.Second), 3),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 40, base.Add(40*time.Second), 4),
	}
	flushAndWait(t, svc, late)

	// Full recomputation: aggregate all samples directly.
	all := append(initial, late...)
	var sum float64
	var count int64
	min := all[0].Value
	max := all[0].Value
	for _, s := range all {
		count++
		sum += s.Value
		if s.Value < min {
			min = s.Value
		}
		if s.Value > max {
			max = s.Value
		}
	}

	view := svc.View()
	ws := base.UnixNano()
	cell, _ := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if cell.Count != count || cell.Sum != sum || cell.Min != min || cell.Max != max {
		t.Fatalf("revision mismatch: got count=%d sum=%v min=%v max=%v, want count=%d sum=%v min=%v max=%v",
			cell.Count, cell.Sum, cell.Min, cell.Max, count, sum, min, max)
	}
}
