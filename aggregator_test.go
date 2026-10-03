package heatmap_test

import (
	"math"
	"math/rand"
	"testing"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func TestAggregator_BasicAggregation(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)

	base := mustTime("2026-01-01T10:00:00Z")
	samples := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(20*time.Second), 2),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 30, base.Add(30*time.Second), 3),
	}
	submitAndSeal(t, svc, base, samples)

	view := svc.View()
	ws := base.UnixNano()
	cell, ok := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if !ok {
		t.Fatal("cell not found")
	}
	if cell.Count != 3 {
		t.Fatalf("count: expected 3, got %d", cell.Count)
	}
	if cell.Sum != 60 {
		t.Fatalf("sum: expected 60, got %v", cell.Sum)
	}
	if cell.Min != 10 {
		t.Fatalf("min: expected 10, got %v", cell.Min)
	}
	if cell.Max != 30 {
		t.Fatalf("max: expected 30, got %v", cell.Max)
	}
}

func TestAggregator_OutOfOrder(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)

	base := mustTime("2026-01-01T10:00:00Z")
	samples := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 30, base.Add(30*time.Second), 3),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(20*time.Second), 2),
	}
	submitAndSeal(t, svc, base, samples)

	view := svc.View()
	ws := base.UnixNano()
	cell, ok := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if !ok {
		t.Fatal("cell not found")
	}
	if cell.Count != 3 || cell.Sum != 60 {
		t.Fatalf("out-of-order aggregation wrong: count=%d sum=%v", cell.Count, cell.Sum)
	}
}

func TestAggregator_LateRevision(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)

	base := mustTime("2026-01-01T10:00:00Z")
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(50*time.Second), 1),
		sealSample(base),
	})

	view := svc.View()
	ws := base.UnixNano()
	cell, ok := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if !ok {
		t.Fatal("cell not found")
	}
	if cell.Count != 1 || cell.Sum != 10 {
		t.Fatalf("sealed cell wrong: count=%d sum=%v", cell.Count, cell.Sum)
	}

	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(40*time.Second), 3),
	})

	view = svc.View()
	cell, ok = view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if !ok {
		t.Fatal("cell not found after revision")
	}
	if cell.Count != 2 {
		t.Fatalf("revised count: expected 2, got %d", cell.Count)
	}
	if cell.Sum != 30 {
		t.Fatalf("revised sum: expected 30, got %v", cell.Sum)
	}
	if cell.Version != 1 {
		t.Fatalf("revised version: expected 1, got %d", cell.Version)
	}

	revs := svc.Revisions()
	if len(revs) != 1 {
		t.Fatalf("expected 1 revision, got %d", len(revs))
	}
	if revs[0].OldVersion != 0 || revs[0].NewVersion != 1 {
		t.Fatalf("revision versions wrong: %d -> %d", revs[0].OldVersion, revs[0].NewVersion)
	}
	if revs[0].Trigger.NodeID != "node-1" || revs[0].Trigger.Seq != 3 {
		t.Fatalf("revision trigger wrong: %+v", revs[0].Trigger)
	}
}

func TestAggregator_DuplicateDetection(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)

	base := mustTime("2026-01-01T10:00:00Z")
	samples := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(20*time.Second), 2),
	}
	submitAndSeal(t, svc, base, samples)

	view := svc.View()
	ws := base.UnixNano()
	cell, _ := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if cell.Count != 2 {
		t.Fatalf("duplicate counted: count=%d", cell.Count)
	}
	if cell.Sum != 30 {
		t.Fatalf("duplicate sum wrong: %v", cell.Sum)
	}
	if svc.Metrics().Duplicates != 1 {
		t.Fatalf("duplicates metric: expected 1, got %d", svc.Metrics().Duplicates)
	}
}

func TestAggregator_Determinism(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 5 * time.Second

	base := mustTime("2026-01-01T10:00:00Z")
	samples := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-2", "pool-1", heatmap.ResourceMemory, 20, base.Add(20*time.Second), 2),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 30, base.Add(30*time.Second), 3),
		makeSample("node-2", "pool-1", heatmap.ResourceMemory, 40, base.Add(40*time.Second), 4),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 50, base.Add(50*time.Second), 5),
	}

	svc1 := newTestService(t, cfg)
	submitAndSeal(t, svc1, base, samples)
	view1 := svc1.View()

	svc2 := newTestService(t, cfg)
	shuffled := make([]heatmap.Sample, len(samples))
	copy(shuffled, samples)
	rand.New(rand.NewSource(99)).Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	submitAndSeal(t, svc2, base, shuffled)
	view2 := svc2.View()

	if len(view1.WindowStarts()) != len(view2.WindowStarts()) {
		t.Fatalf("window count mismatch: %d vs %d", len(view1.WindowStarts()), len(view2.WindowStarts()))
	}
	for _, ws := range view1.WindowStarts() {
		for _, r := range heatmap.AllResources() {
			for _, node := range []string{"node-1", "node-2"} {
				k := heatmap.CellKey{WindowStart: ws, NodeID: node, Resource: r}
				c1, ok1 := view1.CellAt(k)
				c2, ok2 := view2.CellAt(k)
				if ok1 != ok2 {
					t.Fatalf("cell presence mismatch for %v", k)
				}
				if ok1 {
					if c1.Count != c2.Count || c1.Sum != c2.Sum || c1.Min != c2.Min || c1.Max != c2.Max {
						t.Fatalf("cell mismatch for %v: %+v vs %+v", k, c1, c2)
					}
				}
			}
		}
	}
}

func TestAggregator_SealedWindowImmutable(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)

	base := mustTime("2026-01-01T10:00:00Z")
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(50*time.Second), 1),
		sealSample(base),
	})

	view1 := svc.View()
	ws := base.UnixNano()
	cell1, _ := view1.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})

	// A sample in a NEW window should not change the sealed cell.
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(3*time.Minute), 3),
	})

	view2 := svc.View()
	cell2, _ := view2.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if cell1.Count != cell2.Count || cell1.Sum != cell2.Sum {
		t.Fatalf("sealed window changed without revision: count %d->%d sum %v->%v",
			cell1.Count, cell2.Count, cell1.Sum, cell2.Sum)
	}
}

func TestAggregator_MinMaxCorrectness(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)

	base := mustTime("2026-01-01T10:00:00Z")
	values := []float64{5, 1, 9, 3, 7, 2, 8, 4, 6}
	var samples []heatmap.Sample
	for i, v := range values {
		samples = append(samples, makeSample("node-1", "pool-0", heatmap.ResourceCPU, v, base.Add(time.Duration(i)*time.Second), uint64(i+1)))
	}
	submitAndSeal(t, svc, base, samples)

	view := svc.View()
	ws := base.UnixNano()
	cell, _ := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if cell.Min != 1 || cell.Max != 9 {
		t.Fatalf("min/max wrong: min=%v max=%v", cell.Min, cell.Max)
	}
	expectedSum := 0.0
	for _, v := range values {
		expectedSum += v
	}
	if math.Abs(cell.Sum-expectedSum) > 1e-9 {
		t.Fatalf("sum wrong: %v vs %v", cell.Sum, expectedSum)
	}
}

func TestAggregator_MultipleRevisions(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)

	base := mustTime("2026-01-01T10:00:00Z")
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(50*time.Second), 1),
		sealSample(base),
	})

	for i, v := range []float64{20, 30, 40} {
		flushAndWait(t, svc, []heatmap.Sample{
			makeSample("node-1", "pool-0", heatmap.ResourceCPU, v, base.Add(40*time.Second), uint64(10+i)),
		})
	}

	view := svc.View()
	ws := base.UnixNano()
	cell, _ := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if cell.Count != 4 {
		t.Fatalf("count: expected 4, got %d", cell.Count)
	}
	if cell.Sum != 100 {
		t.Fatalf("sum: expected 100, got %v", cell.Sum)
	}
	if cell.Version != 3 {
		t.Fatalf("version: expected 3, got %d", cell.Version)
	}
	revs := svc.Revisions()
	if len(revs) != 3 {
		t.Fatalf("expected 3 revisions, got %d", len(revs))
	}
	for i, r := range revs {
		if r.NewVersion != int64(i+1) {
			t.Fatalf("revision %d version: expected %d, got %d", i, i+1, r.NewVersion)
		}
	}
}
