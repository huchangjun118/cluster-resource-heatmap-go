package heatmap

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		WindowSize:      60 * time.Second,
		SlideStep:       60 * time.Second,
		AllowedLateness: 30 * time.Second,
		RevisionHorizon: 120 * time.Second,
		QueueCapacity:   16,
	}
}

func ts(sec int64) time.Time { return time.Unix(sec, 0).UTC() }

func mkSample(id, node string, sec int64, v float64) Sample {
	return Sample{
		SampleID:  id,
		NodeID:    node,
		Pool:      "pool-a",
		Resource:  ResourceCPU,
		Value:     v,
		Timestamp: ts(sec),
	}
}

// 同一批采样，任意到达顺序产出逐条一致的单元。
func TestAggregatorDeterministicUnderShuffle(t *testing.T) {
	// 补算期足够长，任何顺序都不会触发拒绝，
	// 验证聚合本身与到达顺序、并发度无关。
	cfg := testConfig()
	cfg.RevisionHorizon = time.Hour
	var samples []Sample
	for i := 0; i < 50; i++ {
		samples = append(samples, mkSample(fmt.Sprintf("s%03d", i), fmt.Sprintf("n%d", i%3), int64(i*5), float64(i)))
	}
	reference := ingestAll(cfg, samples)

	for trial := 0; trial < 20; trial++ {
		shuffled := append([]Sample(nil), samples...)
		rand.New(rand.NewSource(int64(trial))).Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		got := ingestAll(cfg, shuffled)
		if len(got) != len(reference) {
			t.Fatalf("trial %d: cell count %d != %d", trial, len(got), len(reference))
		}
		for i := range reference {
			// Version 记录修订次数，与到达历史相关；
			// 聚合内容（窗口/维度/Count/Sum/Min/Max/Sealed）必须逐条一致。
			g, w := got[i], reference[i]
			g.Version, w.Version = 0, 0
			if g != w {
				t.Fatalf("trial %d: cell %d mismatch:\n got %+v\nwant %+v", trial, i, got[i], reference[i])
			}
		}
	}
}

func ingestAll(cfg Config, samples []Sample) []Cell {
	a := newAggregator(cfg)
	for _, s := range samples {
		a.ingest(s)
	}
	return a.snapshot().Cells()
}

// 重复 SampleID 不得计入两次。
func TestAggregatorDuplicate(t *testing.T) {
	a := newAggregator(testConfig())
	a.ingest(mkSample("x", "n0", 10, 1.5))
	r := a.ingest(mkSample("x", "n0", 10, 9.9))
	if !r.duplicate || r.accepted {
		t.Fatalf("expected duplicate, got %+v", r)
	}
	cells := a.snapshot().Cells()
	if len(cells) != 1 || cells[0].Count != 1 || cells[0].Sum != 1.5 {
		t.Fatalf("duplicate counted: %+v", cells)
	}
}

// 水位推进后窗口封口；封口后迟到采样触发修订，结果与全量直算一致。
func TestAggregatorSealAndRevise(t *testing.T) {
	cfg := testConfig()
	a := newAggregator(cfg)
	a.ingest(mkSample("a", "n0", 5, 1.0))
	a.ingest(mkSample("b", "n0", 10, 2.0))
	// 推进水位越过窗口 [0,60)：maxEvent=100 -> watermark=70 >= 60
	r := a.ingest(mkSample("c", "n0", 100, 3.0))
	if len(r.sealed) != 1 {
		t.Fatalf("expected 1 sealed cell, got %+v", r.sealed)
	}
	sealed := r.sealed[0]
	if sealed.Count != 2 || sealed.Sum != 3.0 || sealed.Version != 1 || !sealed.Sealed {
		t.Fatalf("bad sealed cell: %+v", sealed)
	}

	// 迟到补算：窗口 [0,60) 内再来一条
	r2 := a.ingest(mkSample("d", "n0", 20, 4.0))
	if !r2.accepted || len(r2.revisions) != 1 || len(r2.revised) != 1 {
		t.Fatalf("expected revision, got %+v", r2)
	}
	revised := r2.revised[0]
	if revised.Version != 2 || revised.Count != 3 || revised.Sum != 7.0 || revised.Min != 1.0 || revised.Max != 4.0 {
		t.Fatalf("bad revised cell: %+v", revised)
	}

	// 与全量直算一致
	fresh := newAggregator(cfg)
	fresh.ingest(mkSample("a", "n0", 5, 1.0))
	fresh.ingest(mkSample("b", "n0", 10, 2.0))
	fresh.ingest(mkSample("d", "n0", 20, 4.0))
	direct := fresh.snapshot().Cells()
	if len(direct) != 1 {
		t.Fatalf("direct: %+v", direct)
	}
	d := direct[0]
	if d.Count != revised.Count || d.Sum != revised.Sum || d.Min != revised.Min || d.Max != revised.Max {
		t.Fatalf("revision %+v != direct %+v", revised, d)
	}

	// 修订记录可追溯
	revs := a.snapshot().Revisions()
	if len(revs) != 1 || revs[0].Version != 2 || revs[0].Reason != "late-sample" {
		t.Fatalf("bad revisions: %+v", revs)
	}
}

// 封口后无修订时单元不得再变化；越过补算期的迟到被拒绝且可观测。
func TestAggregatorSealedStableAndTooLate(t *testing.T) {
	cfg := testConfig()
	a := newAggregator(cfg)
	a.ingest(mkSample("a", "n0", 5, 1.0))
	a.ingest(mkSample("b", "n0", 100, 2.0)) // watermark=70, 封口 [0,60)
	before := a.snapshot().Cells()

	// 无修订推进：窗口 [0,60) 单元不变
	a.ingest(mkSample("c", "n0", 150, 9.0))
	after := a.snapshot().Cells()
	for i := range before {
		if before[i].Window.Start == ts(0) {
			for j := range after {
				if after[j].Window.Start == ts(0) && after[j] != before[i] {
					t.Fatalf("sealed cell changed without revision: %+v -> %+v", before[i], after[j])
				}
			}
		}
	}

	// 推进水位越过 [0,60) 的补算期：maxEvent=250 -> watermark=220 >= 60+120
	a.ingest(mkSample("d", "n0", 250, 1.0))
	r := a.ingest(mkSample("e", "n0", 30, 5.0))
	if !r.tooLate || r.accepted {
		t.Fatalf("expected tooLate, got %+v", r)
	}
	for _, c := range a.snapshot().Cells() {
		if c.Window.Start == ts(0) && c.Count != 1 {
			t.Fatalf("too-late sample leaked into purged window: %+v", c)
		}
	}
}

// 滑动窗口下采样归属多个窗口。
func TestAggregatorSlidingWindows(t *testing.T) {
	cfg := testConfig()
	cfg.WindowSize = 60 * time.Second
	cfg.SlideStep = 20 * time.Second
	a := newAggregator(cfg)
	a.ingest(mkSample("a", "n0", 50, 2.0))
	cells := a.snapshot().Cells()
	// t=50 属于起点为 40,20,0 的三个窗口
	if len(cells) != 3 {
		t.Fatalf("want 3 cells, got %+v", cells)
	}
	for _, c := range cells {
		if c.Count != 1 || c.Sum != 2.0 {
			t.Fatalf("bad sliding cell: %+v", c)
		}
	}
}
