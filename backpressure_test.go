package heatmap_test

import (
	"context"
	"testing"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func TestBackpressure_BoundedQueue(t *testing.T) {
	cfg := testConfig()
	cfg.QueueCapacity = 10
	cfg.DropPolicy = heatmap.DropPolicyDrop
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	heatmap.SetSimulateProcessingDelay(50 * time.Millisecond)
	defer heatmap.SetSimulateProcessingDelay(0)

	submitted := 0
	dropped := 0
	for i := 0; i < 100; i++ {
		s := makeSample("node-1", "pool-0", heatmap.ResourceCPU, float64(i), base.Add(time.Duration(i)*time.Second), uint64(i))
		if err := svc.Submit(s); err != nil {
			dropped++
		} else {
			submitted++
		}
	}
	if dropped == 0 {
		t.Fatal("expected some samples to be dropped")
	}
	t.Logf("submitted=%d dropped=%d", submitted, dropped)
}

func TestBackpressure_BlockPolicy(t *testing.T) {
	cfg := testConfig()
	cfg.QueueCapacity = 5
	cfg.DropPolicy = heatmap.DropPolicyBlock
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	heatmap.SetSimulateProcessingDelay(500 * time.Millisecond)
	defer heatmap.SetSimulateProcessingDelay(0)

	// Submit many samples quickly to fill the queue and block.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	blocked := false
	for i := 0; i < 100; i++ {
		s := makeSample("node-1", "pool-0", heatmap.ResourceCPU, float64(i), base.Add(time.Duration(i)*time.Second), uint64(i))
		err := svc.SubmitContext(ctx, s)
		if err != nil {
			blocked = true
			break
		}
	}
	if !blocked {
		t.Fatal("expected Submit to block and time out")
	}
}

func TestBackpressure_Metrics(t *testing.T) {
	cfg := testConfig()
	cfg.QueueCapacity = 100
	cfg.DropPolicy = heatmap.DropPolicyDrop
	svc := newTestService(t, cfg)
	base := mustTime("2026-01-01T10:00:00Z")

	heatmap.SetSimulateProcessingDelay(10 * time.Millisecond)
	defer heatmap.SetSimulateProcessingDelay(0)

	for i := 0; i < 200; i++ {
		s := makeSample("node-1", "pool-0", heatmap.ResourceCPU, float64(i), base.Add(time.Duration(i)*time.Second), uint64(i))
		svc.Submit(s)
	}
	time.Sleep(50 * time.Millisecond)
	m := svc.Metrics()
	if m.SamplesDropped == 0 {
		t.Fatal("expected drops with full queue")
	}
	t.Logf("metrics: %+v", m)
}
