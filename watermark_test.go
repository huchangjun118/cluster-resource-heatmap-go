package heatmap_test

import (
	"context"
	"testing"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func TestWatermark_AllowedLateness(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 10 * time.Second
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	// Sample at base+50s. Watermark = base+50s - 10s = base+40s.
	// Window [base, base+60s) end = base+60s. base+60s <= base+40s? No. Not sealed.
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(50*time.Second), 1),
	})
	view := svc.View()
	if len(view.WindowStarts()) != 0 {
		t.Fatal("window should not be sealed yet")
	}

	// Sample at base+70s. Watermark = base+70s - 10s = base+60s.
	// Window end = base+60s <= base+60s? Yes. Sealed.
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(70*time.Second), 2),
	})
	view = svc.View()
	if len(view.WindowStarts()) != 1 {
		t.Fatalf("expected 1 sealed window, got %d", len(view.WindowStarts()))
	}
}

func TestWatermark_LateDataAfterSeal(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 10, base.Add(50*time.Second), 1),
		sealSample(base),
	})

	// Late data after seal should revise.
	flushAndWait(t, svc, []heatmap.Sample{
		makeSample("node-1", "pool-0", heatmap.ResourceCPU, 20, base.Add(40*time.Second), 2),
	})

	m := svc.Metrics()
	if m.LateRevisions != 1 {
		t.Fatalf("expected 1 late revision, got %d", m.LateRevisions)
	}
}

func TestForceSeal_MaxOpenWindows(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = time.Hour // large lateness so watermark doesn't seal windows
	cfg.MaxOpenWindows = 3
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	// Submit samples in many different windows.
	for i := 0; i < 10; i++ {
		s := makeSample("node-1", "pool-0", heatmap.ResourceCPU, float64(i),
			base.Add(time.Duration(i)*time.Minute), uint64(i+1))
		svc.Submit(s)
	}
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	m := svc.Metrics()
	// Force seals should have occurred.
	if m.ForcedSeals == 0 {
		t.Fatal("expected force seals")
	}
	// Open windows should be bounded.
	if m.OpenWindows > int64(cfg.MaxOpenWindows) {
		t.Fatalf("open windows %d exceeds max %d", m.OpenWindows, cfg.MaxOpenWindows)
	}
}

func TestContextCancellation_NoPartialWrites(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedLateness = 0
	cfg.QueueCapacity = 5
	cfg.DropPolicy = heatmap.DropPolicyBlock
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	heatmap.SetSimulateProcessingDelay(100 * time.Millisecond)
	defer heatmap.SetSimulateProcessingDelay(0)

	// Fill the queue.
	for i := 0; i < 5; i++ {
		s := makeSample("node-1", "pool-0", heatmap.ResourceCPU, float64(i),
			base.Add(time.Duration(i)*time.Second), uint64(i+1))
		svc.Submit(s)
	}

	// Submit with a canceled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := makeSample("node-1", "pool-0", heatmap.ResourceCPU, 999, base.Add(100*time.Second), 999)
	err := svc.SubmitContext(ctx, s)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}

	// The service should still be consistent.
	if err := svc.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	view := svc.View()
	// No partial window state: each sealed cell should have count >= 1.
	for _, ws := range view.WindowStarts() {
		cells := view.Heatmap(time.Unix(0, ws), time.Unix(0, ws).Add(time.Minute))
		for _, c := range cells {
			if c.Count < 1 {
				t.Fatalf("partial cell: count=%d", c.Count)
			}
		}
	}
}
