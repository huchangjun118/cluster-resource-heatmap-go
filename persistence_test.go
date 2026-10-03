package heatmap_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func tempPaths(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "wal"), filepath.Join(dir, "snap")
}

func TestPersistence_RestartRecovery(t *testing.T) {
	walPath, snapPath := tempPaths(t)
	cfg := testConfig()
	cfg.AllowedLateness = 0
	cfg.WALPath = walPath
	cfg.SnapshotPath = snapPath

	base := mustTime("2026-01-01T10:00:00Z")
	samples := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(20*time.Second), 2),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 30, base.Add(30*time.Second), 3),
	}

	// First service: process samples, seal, snapshot.
	svc1, err := heatmap.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc1.Start(); err != nil {
		t.Fatal(err)
	}
	flushAndWait(t, svc1, samples)
	flushAndWait(t, svc1, []heatmap.Sample{sealSample(base)})
	if err := svc1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	view1 := svc1.View()
	svc1.Stop()

	// Second service: recover and verify.
	svc2, err := heatmap.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc2.Start(); err != nil {
		t.Fatal(err)
	}
	defer svc2.Stop()

	view2 := svc2.View()
	if len(view1.WindowStarts()) != len(view2.WindowStarts()) {
		t.Fatalf("window count mismatch: %d vs %d", len(view1.WindowStarts()), len(view2.WindowStarts()))
	}
	ws := base.UnixNano()
	cell1, _ := view1.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	cell2, _ := view2.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if cell1.Count != cell2.Count || cell1.Sum != cell2.Sum {
		t.Fatalf("recovery mismatch: count %d->%d sum %v->%v",
			cell1.Count, cell2.Count, cell1.Sum, cell2.Sum)
	}
}

func TestPersistence_NoDoubleCounting(t *testing.T) {
	walPath, snapPath := tempPaths(t)
	cfg := testConfig()
	cfg.AllowedLateness = 0
	cfg.WALPath = walPath
	cfg.SnapshotPath = snapPath

	base := mustTime("2026-01-01T10:00:00Z")
	samples := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(20*time.Second), 2),
	}

	svc1, err := heatmap.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc1.Start(); err != nil {
		t.Fatal(err)
	}
	flushAndWait(t, svc1, samples)
	flushAndWait(t, svc1, []heatmap.Sample{sealSample(base)})
	if err := svc1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	svc1.Stop()

	// Recover and submit the SAME samples again (simulating WAL replay + duplicate).
	svc2, err := heatmap.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc2.Start(); err != nil {
		t.Fatal(err)
	}
	defer svc2.Stop()

	// Re-submit the same samples (they should be detected as duplicates).
	flushAndWait(t, svc2, samples)
	// Also submit a new sample.
	flushAndWait(t, svc2, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 30, base.Add(30*time.Second), 3),
	})

	view := svc2.View()
	ws := base.UnixNano()
	cell, _ := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	// Should be 3 (10+20+30), not 6 (double counted).
	if cell.Count != 3 {
		t.Fatalf("double counting: count=%d", cell.Count)
	}
	if cell.Sum != 60 {
		t.Fatalf("double counting: sum=%v", cell.Sum)
	}
}

func TestPersistence_RevisionsRecovered(t *testing.T) {
	walPath, snapPath := tempPaths(t)
	cfg := testConfig()
	cfg.AllowedLateness = 0
	cfg.WALPath = walPath
	cfg.SnapshotPath = snapPath

	base := mustTime("2026-01-01T10:00:00Z")

	svc1, err := heatmap.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc1.Start(); err != nil {
		t.Fatal(err)
	}
	flushAndWait(t, svc1, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(50*time.Second), 1),
		sealSample(base),
	})
	// Late revision.
	flushAndWait(t, svc1, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(40*time.Second), 2),
	})
	if err := svc1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	revs1 := svc1.Revisions()
	svc1.Stop()

	svc2, err := heatmap.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc2.Start(); err != nil {
		t.Fatal(err)
	}
	defer svc2.Stop()

	revs2 := svc2.Revisions()
	if len(revs2) != len(revs1) {
		t.Fatalf("revision count mismatch: %d vs %d", len(revs1), len(revs2))
	}
	if len(revs2) != 1 {
		t.Fatalf("expected 1 revision, got %d", len(revs2))
	}
	if revs2[0].NewVersion != 1 {
		t.Fatalf("recovered revision version: expected 1, got %d", revs2[0].NewVersion)
	}

	// Verify the cell has the revision.
	view := svc2.View()
	ws := base.UnixNano()
	cell, _ := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if cell.Version != 1 || cell.Count != 2 || cell.Sum != 30 {
		t.Fatalf("recovered cell wrong: version=%d count=%d sum=%v", cell.Version, cell.Count, cell.Sum)
	}
}

func TestPersistence_WALReplay(t *testing.T) {
	walPath, snapPath := tempPaths(t)
	cfg := testConfig()
	cfg.AllowedLateness = 0
	cfg.WALPath = walPath
	cfg.SnapshotPath = snapPath

	base := mustTime("2026-01-01T10:00:00Z")
	samples := []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(20*time.Second), 2),
	}

	// First service: process but DON'T snapshot (so WAL has all entries).
	svc1, err := heatmap.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc1.Start(); err != nil {
		t.Fatal(err)
	}
	flushAndWait(t, svc1, samples)
	flushAndWait(t, svc1, []heatmap.Sample{sealSample(base)})
	// No snapshot - WAL has all entries.
	svc1.Stop()

	// Second service: recover from WAL (no snapshot).
	svc2, err := heatmap.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc2.Start(); err != nil {
		t.Fatal(err)
	}
	defer svc2.Stop()

	view := svc2.View()
	ws := base.UnixNano()
	cell, ok := view.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if !ok {
		t.Fatal("cell not found after WAL replay")
	}
	if cell.Count != 2 || cell.Sum != 30 {
		t.Fatalf("WAL replay wrong: count=%d sum=%v", cell.Count, cell.Sum)
	}
}

func TestPersistence_NoSnapshotFile(t *testing.T) {
	walPath, snapPath := tempPaths(t)
	// Remove snapshot file if it exists.
	os.Remove(snapPath)

	cfg := testConfig()
	cfg.WALPath = walPath
	cfg.SnapshotPath = snapPath

	svc, err := heatmap.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(); err != nil {
		t.Fatal(err)
	}
	defer svc.Stop()

	// Should start with empty state.
	view := svc.View()
	if len(view.WindowStarts()) != 0 {
		t.Fatal("expected empty state")
	}
}
