package heatmap_test

import (
	"testing"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func TestSlidingWindow_Basic(t *testing.T) {
	cfg := testConfig()
	cfg.WindowSize = time.Minute
	cfg.WindowSlide = 20 * time.Second
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	// One sample should appear in 3 windows.
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(30*time.Second), 1),
		sealSample(base),
	})

	view := svc.View()
	// The sample at base+30s is in windows: [base, base+60s), [base-20s, base+40s), [base-40s, base+20s).
	// After sealing, the watermark is base+2min, so all these windows are sealed.
	// But the seal sample is at base+2min, which is in [base+2min, base+3min).
	// The windows containing base+30s are sealed (end <= base+2min).
	count := 0
	for _, ws := range view.WindowStarts() {
		cell, ok := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
		if ok && cell.Count == 1 {
			count++
		}
	}
	if count != 3 {
		t.Fatalf("expected sample in 3 windows, got %d", count)
	}
}

func TestSlidingWindow_NoDoubleCount(t *testing.T) {
	cfg := testConfig()
	cfg.WindowSize = time.Minute
	cfg.WindowSlide = 20 * time.Second
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	// Multiple samples.
	samples := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(30*time.Second), 2),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 30, base.Add(50*time.Second), 3),
	}
	flushAndWait(t, svc, samples)
	flushAndWait(t, svc, []heatmap.Sample{sealSample(base)})

	view := svc.View()
	// Each sample appears in 3 windows. Total count across all cells should be 3*3=9.
	var totalCount int64
	for _, ws := range view.WindowStarts() {
		cell, ok := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
		if ok {
			totalCount += cell.Count
		}
	}
	if totalCount != 9 {
		t.Fatalf("expected total count 9 (3 samples * 3 windows), got %d", totalCount)
	}
}

func TestSlidingWindow_Revision(t *testing.T) {
	cfg := testConfig()
	cfg.WindowSize = time.Minute
	cfg.WindowSlide = 20 * time.Second
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(30*time.Second), 1),
		sealSample(base),
	})

	// Late revision for the sealed windows.
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(25*time.Second), 2),
	})

	view := svc.View()
	// The late sample should revise all 3 windows it belongs to.
	revisedCount := 0
	for _, ws := range view.WindowStarts() {
		cell, ok := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
		if ok && cell.Count == 2 {
			revisedCount++
		}
	}
	if revisedCount != 3 {
		t.Fatalf("expected 3 revised windows, got %d", revisedCount)
	}
}
