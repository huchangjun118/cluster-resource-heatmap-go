package heatmap_test

import (
	"sync"
	"testing"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func TestConcurrency_QueriesWhileAggregating(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	// Start a writer that continuously submits samples.
	var wg sync.WaitGroup
	stopWrite := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stopWrite:
				return
			default:
			}
			i++
			s := makeSample("node-1", "pool-0", heatmap.ResourceCPU, float64(i), base.Add(time.Duration(i)*time.Second), uint64(i))
			if err := svc.Submit(s); err != nil {
				return
			}
			time.Sleep(time.Microsecond)
		}
	}()

	// Start readers that continuously query.
	stopRead := make(chan struct{})
	var readWg sync.WaitGroup
	for r := 0; r < 5; r++ {
		readWg.Add(1)
		go func() {
			defer readWg.Done()
			for {
				select {
				case <-stopRead:
					return
				default:
				}
				view := svc.View()
				_ = view.Heatmap(base, base.Add(time.Hour))
				_ = view.QueryByNode("node-1", base, base.Add(time.Hour))
			}
		}()
	}

	// Let it run for a bit.
	time.Sleep(100 * time.Millisecond)
	close(stopWrite)
	wg.Wait()
	close(stopRead)
	readWg.Wait()

	// Final flush and verify no errors.
	if err := svc.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	view := svc.View()
	if len(view.WindowStarts()) == 0 {
		t.Fatal("no windows produced")
	}
}

func TestConcurrency_ViewIsImmutable(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	// Submit and seal a window.
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(10*time.Second), 1),
		sealSample(base),
	})

	view1 := svc.View()
	ws := base.UnixNano()
	cell1, _ := view1.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})

	// Submit a late revision.
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(20*time.Second), 2),
	})

	// view1 should be unchanged (immutable).
	cell1After, _ := view1.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if cell1After.Count != cell1.Count || cell1After.Sum != cell1.Sum {
		t.Fatalf("view changed after revision: count %d->%d sum %v->%v",
			cell1.Count, cell1After.Count, cell1.Sum, cell1After.Sum)
	}

	// view2 should have the revision.
	view2 := svc.View()
	cell2, _ := view2.CellAt(heatmap.CellKey{WindowStart: ws, NodeID: "node-1", Resource: heatmap.ResourceCPU})
	if cell2.Count != 2 || cell2.Sum != 30 {
		t.Fatalf("revised view wrong: count=%d sum=%v", cell2.Count, cell2.Sum)
	}
}

func TestConcurrency_NoPartialState(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	// Continuously submit and query. The query should always see a complete view.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				j++
				s := makeSample("node-1", "pool-0", heatmap.ResourceCPU, float64(j), base.Add(time.Duration(j)*time.Second), uint64(j))
				svc.Submit(s)
			}
		}()
	}

	// Query and verify consistency: every cell should have count >= 1 and sum >= count*min.
	for i := 0; i < 100; i++ {
		view := svc.View()
		for _, ws := range view.WindowStarts() {
			cells := view.Heatmap(time.Unix(0, ws), time.Unix(0, ws).Add(time.Minute))
			for _, c := range cells {
				if c.Count < 1 {
					t.Fatalf("cell with count %d", c.Count)
				}
				if c.Sum < float64(c.Count)*c.Min-1e-9 {
					t.Fatalf("inconsistent cell: count=%d sum=%v min=%v", c.Count, c.Sum, c.Min)
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
}
