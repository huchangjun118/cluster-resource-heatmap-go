package heatmap_test

import (
	"testing"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func makeSample(node, pool string, r heatmap.ResourceType, v float64, t time.Time, seq uint64) heatmap.Sample {
	return heatmap.Sample{
		NodeID:    node,
		PoolID:    pool,
		Resource:  r,
		Value:     v,
		EventTime: t,
		Seq:       seq,
	}
}

func newTestService(t *testing.T, cfg heatmap.Config) *heatmap.Service {
	t.Helper()
	svc, err := heatmap.NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { svc.Stop() })
	return svc
}

func testConfig() heatmap.Config {
	return heatmap.Config{
		WindowSize:      time.Minute,
		WindowSlide:     time.Minute,
		AllowedLateness: 10 * time.Second,
		MaxOpenWindows:  100,
		MaxSeen:         10000,
		QueueCapacity:   10000,
		DropPolicy:      heatmap.DropPolicyBlock,
	}
}

// flushAndWait submits samples and waits for processing.
func flushAndWait(t *testing.T, svc *heatmap.Service, samples []heatmap.Sample) {
	t.Helper()
	for _, s := range samples {
		if err := svc.Submit(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// sealSample returns a sample that advances the watermark past the first window.
func sealSample(base time.Time) heatmap.Sample {
	return makeSample("__seal__", "pool-0", heatmap.ResourceCPU, 0, base.Add(2*time.Minute), 999999)
}

// submitAndSeal submits samples then a sealing sample, and waits.
func submitAndSeal(t *testing.T, svc *heatmap.Service, base time.Time, samples []heatmap.Sample) {
	t.Helper()
	flushAndWait(t, svc, samples)
	flushAndWait(t, svc, []heatmap.Sample{sealSample(base)})
}

// SetSimulateProcessingDelay sets the processing delay for backpressure tests.
func SetSimulateProcessingDelay(d time.Duration) {
	heatmap.SetSimulateProcessingDelay(d)
}
