package heatmap_test

import (
	"testing"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func setupQueryTest(t *testing.T) (*heatmap.Service, time.Time) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")
	samples := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceMemory, 20, base.Add(20*time.Second), 2),
		makeSample("node-2", "pool-1", heatmap.ResourceCPU, 30, base.Add(30*time.Second), 3),
		makeSample("node-2", "pool-1", heatmap.ResourceMemory, 40, base.Add(40*time.Second), 4),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 50, base.Add(50*time.Second), 5),
	}
	submitAndSeal(t, svc, base, samples)
	return svc, base
}

func TestQuery_Heatmap(t *testing.T) {
	svc, base := setupQueryTest(t)
	view := svc.View()
	cells := view.Heatmap(base, base.Add(time.Minute))
	if len(cells) != 4 {
		t.Fatalf("expected 4 cells, got %d", len(cells))
	}
}

func TestQuery_ByNode(t *testing.T) {
	svc, base := setupQueryTest(t)
	view := svc.View()
	cells := view.QueryByNode("node-1", base, base.Add(time.Minute))
	if len(cells) != 2 {
		t.Fatalf("expected 2 cells for node-1, got %d", len(cells))
	}
	var cpuCount, memCount int64
	for _, c := range cells {
		if c.Resource == heatmap.ResourceCPU {
			cpuCount = c.Count
		}
		if c.Resource == heatmap.ResourceMemory {
			memCount = c.Count
		}
	}
	if cpuCount != 2 {
		t.Fatalf("node-1 CPU count: expected 2, got %d", cpuCount)
	}
	if memCount != 1 {
		t.Fatalf("node-1 memory count: expected 1, got %d", memCount)
	}
}

func TestQuery_ByPool(t *testing.T) {
	svc, base := setupQueryTest(t)
	view := svc.View()
	cells := view.QueryByPool("pool-0", base, base.Add(time.Minute))
	// pool-0 has node-1 with CPU and memory. Within the window, cells are merged by (node, resource).
	// So we get 2 cells: (node-1, CPU) and (node-1, memory).
	if len(cells) != 2 {
		t.Fatalf("expected 2 cells for pool-0, got %d", len(cells))
	}
	// Verify pool-0 aggregates node-1 only.
	for _, c := range cells {
		if c.NodeID != "node-1" {
			t.Fatalf("pool-0 cell has wrong node: %s", c.NodeID)
		}
	}
}

func TestQuery_ByResource(t *testing.T) {
	svc, base := setupQueryTest(t)
	view := svc.View()
	cells := view.QueryByResource(heatmap.ResourceCPU, base, base.Add(time.Minute))
	if len(cells) != 2 {
		t.Fatalf("expected 2 CPU cells, got %d", len(cells))
	}
	for _, c := range cells {
		if c.Resource != heatmap.ResourceCPU {
			t.Fatalf("wrong resource: %s", c.Resource)
		}
	}
}

func TestQuery_SelfConsistency(t *testing.T) {
	svc, base := setupQueryTest(t)
	view := svc.View()

	// Heatmap total count.
	heatmapCells := view.Heatmap(base, base.Add(time.Minute))
	var totalCount int64
	for _, c := range heatmapCells {
		totalCount += c.Count
	}

	// Pool-level: sum of pool cells should equal total.
	pool0 := view.QueryByPool("pool-0", base, base.Add(time.Minute))
	pool1 := view.QueryByPool("pool-1", base, base.Add(time.Minute))
	var poolCount int64
	for _, c := range pool0 {
		poolCount += c.Count
	}
	for _, c := range pool1 {
		poolCount += c.Count
	}
	if poolCount != totalCount {
		t.Fatalf("pool count %d != heatmap count %d", poolCount, totalCount)
	}

	// Resource-level: sum of resource cells should equal total.
	var resCount int64
	for _, r := range heatmap.AllResources() {
		rc := view.QueryByResource(r, base, base.Add(time.Minute))
		for _, c := range rc {
			resCount += c.Count
		}
	}
	if resCount != totalCount {
		t.Fatalf("resource count %d != heatmap count %d", resCount, totalCount)
	}
}

func TestQuery_TimeRange(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")
	samples := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(70*time.Second), 2), // next window
	}
	submitAndSeal(t, svc, base, samples)

	view := svc.View()
	// Query only the first window.
	cells := view.Heatmap(base, base.Add(time.Minute))
	if len(cells) != 1 {
		t.Fatalf("expected 1 cell in first window, got %d", len(cells))
	}
	// Query both windows.
	cells = view.Heatmap(base, base.Add(2*time.Minute))
	if len(cells) != 2 {
		t.Fatalf("expected 2 cells in both windows, got %d", len(cells))
	}
}

func TestQuery_EmptyRange(t *testing.T) {
	svc, base := setupQueryTest(t)
	view := svc.View()
	cells := view.Heatmap(base.Add(10*time.Minute), base.Add(11*time.Minute))
	if len(cells) != 0 {
		t.Fatalf("expected 0 cells for empty range, got %d", len(cells))
	}
}
