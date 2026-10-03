package heatmap

import (
	"testing"
	"time"
)

func TestWindowAssignTumbling(t *testing.T) {
	a := newWindowAssigner(time.Minute, time.Minute)
	ts := time.Unix(0, 90_000_000_000).UTC() // 00:01:30
	ws := a.assign(ts)
	if len(ws) != 1 {
		t.Fatalf("want 1 window, got %d", len(ws))
	}
	if ws[0].Start != time.Unix(0, 60_000_000_000).UTC() || ws[0].End != time.Unix(0, 120_000_000_000).UTC() {
		t.Fatalf("bad window: %+v", ws[0])
	}
}

func TestWindowAssignSliding(t *testing.T) {
	a := newWindowAssigner(60*time.Second, 20*time.Second)
	ts := time.Unix(0, 50_000_000_000).UTC() // 00:00:50
	ws := a.assign(ts)
	// 覆盖 50s 的窗口起点: 40s, 20s, 0s（起点 -20s 的窗口结束于 40s 不覆盖）
	if len(ws) != 3 {
		t.Fatalf("want 3 windows, got %d: %v", len(ws), ws)
	}
	for i, w := range ws {
		wantStart := time.Unix(0, int64(40-20*i)*int64(time.Second)).UTC()
		if w.Start != wantStart {
			t.Fatalf("window %d start = %v, want %v", i, w.Start, wantStart)
		}
		if !(w.Start.Before(ts) || w.Start.Equal(ts)) || !w.End.After(ts) {
			t.Fatalf("window %+v does not contain %v", w, ts)
		}
	}
}

func TestWindowBoundaryDeterministic(t *testing.T) {
	a := newWindowAssigner(10*time.Second, 5*time.Second)
	// 恰好落在窗口边界上的点：属于以它为起点的窗口，不属于以它为终点的窗口。
	ts := time.Unix(0, 10_000_000_000).UTC()
	ws := a.assign(ts)
	for _, w := range ws {
		if w.End.Equal(ts) {
			t.Fatalf("half-open violated: %+v ends at ts", w)
		}
	}
	if ws[0].Start != ts {
		t.Fatalf("latest window should start at ts, got %v", ws[0].Start)
	}
}
