package heatmap_test

import (
	"testing"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func TestWindowAssigner_Tumbling(t *testing.T) {
	a, err := heatmap.NewWindowAssigner(time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !a.IsTumbling() {
		t.Fatal("expected tumbling")
	}
	// A sample at 10:00:30 should be in [10:00, 10:01).
	w := a.WindowsFor(mustTime("2026-01-01T10:00:30Z"))
	if len(w) != 1 {
		t.Fatalf("expected 1 window, got %d", len(w))
	}
	want := mustTime("2026-01-01T10:00:00Z")
	if !w[0].Start.Equal(want) {
		t.Fatalf("expected start %v, got %v", want, w[0].Start)
	}
	// Boundary: 10:01:00 is in the next window.
	w2 := a.WindowsFor(mustTime("2026-01-01T10:01:00Z"))
	if !w2[0].Start.Equal(mustTime("2026-01-01T10:01:00Z")) {
		t.Fatalf("boundary mismatch: %v", w2[0].Start)
	}
}

func TestWindowAssigner_Sliding(t *testing.T) {
	a, err := heatmap.NewWindowAssigner(time.Minute, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if a.IsTumbling() {
		t.Fatal("expected sliding")
	}
	// 60s / 20s = 3 windows per event.
	if a.NumWindows() != 3 {
		t.Fatalf("expected 3 windows, got %d", a.NumWindows())
	}
	w := a.WindowsFor(mustTime("2026-01-01T10:00:30Z"))
	if len(w) != 3 {
		t.Fatalf("expected 3 windows, got %d", len(w))
	}
	// Windows should be ascending and 20s apart.
	for i := 1; i < len(w); i++ {
		if w[i].Start.Sub(w[i-1].Start) != 20*time.Second {
			t.Fatalf("windows not 20s apart: %v", w)
		}
	}
	// All windows should contain the event.
	for _, win := range w {
		if !win.Contains(mustTime("2026-01-01T10:00:30Z")) {
			t.Fatalf("window %v does not contain event", win)
		}
	}
}

func TestWindowAssigner_Deterministic(t *testing.T) {
	a, err := heatmap.NewWindowAssigner(time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t1 := mustTime("2026-01-01T10:00:30Z")
	t2 := mustTime("2026-01-01T10:00:30Z")
	w1 := a.WindowsFor(t1)
	w2 := a.WindowsFor(t2)
	if len(w1) != len(w2) || !w1[0].Start.Equal(w2[0].Start) {
		t.Fatal("non-deterministic window assignment")
	}
}

func TestWindowAssigner_Invalid(t *testing.T) {
	cases := []struct{ size, slide time.Duration }{
		{0, time.Second},
		{time.Second, 0},
		{time.Second, 2 * time.Second},
		{time.Second, 300 * time.Millisecond}, // size not multiple of slide
	}
	for _, c := range cases {
		if _, err := heatmap.NewWindowAssigner(c.size, c.slide); err == nil {
			t.Fatalf("expected error for size=%v slide=%v", c.size, c.slide)
		}
	}
}

func TestWindow_Contains(t *testing.T) {
	w := heatmap.Window{Start: mustTime("2026-01-01T10:00:00Z"), End: mustTime("2026-01-01T10:01:00Z")}
	if !w.Contains(mustTime("2026-01-01T10:00:00Z")) {
		t.Fatal("should contain start")
	}
	if !w.Contains(mustTime("2026-01-01T10:00:59Z")) {
		t.Fatal("should contain time inside")
	}
	if w.Contains(mustTime("2026-01-01T10:01:00Z")) {
		t.Fatal("should not contain end")
	}
}
