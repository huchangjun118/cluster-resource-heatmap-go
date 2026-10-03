package heatmap

import (
	"sort"
	"time"
)

// 聚合引擎：单 goroutine 持有全部可变状态，
// 通过不可变快照对外提供查询视图。
//
// 不变量：
//   - 窗口归属只由 (WindowSize, SlideStep, 事件时间) 决定，与到达顺序无关；
//   - 水位 = 已见最大事件时间 - AllowedLateness，单调不减；
//   - 窗口 End <= 水位时封口，封口后单元只能通过修订变化；
//   - 窗口 End + RevisionHorizon <= 水位时清理原始采样，此后迟到采样被拒绝（TooLate）。

// cellKey 单元键。
type cellKey struct {
	windowStart time.Time
	nodeID      string
	resource    ResourceType
}

// windowState 单个窗口的中间聚合状态。
// 封口后在 RevisionHorizon 内仍保留原始采样以支持补算。
type windowState struct {
	window  Window
	samples map[cellKey]map[string]Sample // cellKey -> sampleID -> Sample
	sealed  bool
}

// ingestResult 一次摄入的结果，供持久化层落盘与指标统计。
type ingestResult struct {
	accepted  bool       // 被接受（含触发修订）
	duplicate bool       // 重复采样，被忽略
	tooLate   bool       // 超出补算期，被拒绝
	sealed    []Cell     // 本次新封口的单元
	revisions []Revision // 本次产生的修订
	revised   []Cell     // 修订后的单元全量（与 revisions 一一对应）
}

// aggregator 聚合引擎。
type aggregator struct {
	cfg      Config
	assigner windowAssigner

	windows     map[time.Time]*windowState // 未清理的窗口（含封口但未过补算期的）
	sealedCells map[cellKey]Cell           // 全部已封口单元（清理后仍保留，供查询）
	revisions   []Revision                 // 全部修订记录
	seen        map[string]int64           // sampleID -> 事件时间，全局去重

	watermark time.Time
	maxEvent  time.Time
}

func newAggregator(cfg Config) *aggregator {
	return &aggregator{
		cfg:         cfg,
		assigner:    newWindowAssigner(cfg.WindowSize, cfg.SlideStep),
		windows:     make(map[time.Time]*windowState),
		sealedCells: make(map[cellKey]Cell),
		seen:        make(map[string]int64),
	}
}

// ingest 摄入一条采样：去重、归属窗口、推进水位、封口到期窗口、必要时修订。
func (a *aggregator) ingest(s Sample) ingestResult {
	var res ingestResult

	// 全局去重：同一 SampleID 绝不计入两次。
	if _, dup := a.seen[s.SampleID]; dup {
		res.duplicate = true
		return res
	}

	// 推进水位（事件时间水位，单调不减）。
	if s.Timestamp.After(a.maxEvent) {
		a.maxEvent = s.Timestamp
		if wm := a.maxEvent.Add(-a.cfg.AllowedLateness); wm.After(a.watermark) {
			a.watermark = wm
		}
	}

	wins := a.assigner.assign(s.Timestamp)

	// 全部目标窗口都已清理 -> 超出补算期，拒绝（可观测，不静默丢弃）。
	anyAccepted := false
	for _, w := range wins {
		if a.purged(w) {
			continue
		}
		anyAccepted = true
		break
	}
	if !anyAccepted {
		res.tooLate = true
		return res
	}

	a.seen[s.SampleID] = s.Timestamp.UnixNano()

	for _, w := range wins {
		if a.purged(w) {
			continue
		}
		ws := a.windows[w.Start]
		if ws == nil {
			ws = &windowState{window: w, samples: make(map[cellKey]map[string]Sample)}
			a.windows[w.Start] = ws
		}
		ck := cellKey{windowStart: w.Start, nodeID: s.NodeID, resource: s.Resource}
		bucket := ws.samples[ck]
		if bucket == nil {
			bucket = make(map[string]Sample)
			ws.samples[ck] = bucket
		}
		bucket[s.SampleID] = s

		if ws.sealed {
			// 已封口窗口的迟到补算：用窗口内全量采样重算单元，版本 +1。
			cell := foldBucket(w, ck, bucket, a.poolOf(w.Start, s.NodeID, s.Pool))
			prev := a.sealedCells[ck]
			cell.Version = prev.Version + 1
			cell.Sealed = true
			a.sealedCells[ck] = cell
			rev := Revision{
				Window:    w,
				NodeID:    s.NodeID,
				Resource:  s.Resource,
				Version:   cell.Version,
				Reason:    "late-sample",
				RevisedAt: s.Timestamp,
			}
			a.revisions = append(a.revisions, rev)
			res.revisions = append(res.revisions, rev)
			res.revised = append(res.revised, cell)
		}
	}
	res.accepted = true

	// 封口与清理。
	a.sealAndPurge(&res)
	return res
}

// poolOf 解析窗口内某节点的池名（同节点在不同采样中池名应一致，
// 以窗口内字典序最小 SampleID 的采样为准，保证确定性）。
func (a *aggregator) poolOf(windowStart time.Time, nodeID, fallback string) string {
	ws := a.windows[windowStart]
	if ws == nil {
		return fallback
	}
	best := fallback
	bestID := ""
	for ck, bucket := range ws.samples {
		if ck.nodeID != nodeID {
			continue
		}
		for id, smp := range bucket {
			if bestID == "" || id < bestID {
				bestID = id
				best = smp.Pool
			}
		}
	}
	return best
}

// purged 判断窗口是否已清理（不再接受任何补算）。
func (a *aggregator) purged(w Window) bool {
	return !a.watermark.Before(w.End.Add(a.cfg.RevisionHorizon))
}

// sealAndPurge 封口所有 End <= 水位的窗口，并清理越过补算期的窗口。
func (a *aggregator) sealAndPurge(res *ingestResult) {
	starts := make([]time.Time, 0, len(a.windows))
	for st := range a.windows {
		starts = append(starts, st)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })

	for _, st := range starts {
		ws := a.windows[st]
		if !ws.sealed && !a.watermark.Before(ws.window.End) {
			// 封口：从全量采样计算单元，版本 1。
			ws.sealed = true
			cks := make([]cellKey, 0, len(ws.samples))
			for ck := range ws.samples {
				cks = append(cks, ck)
			}
			sort.Slice(cks, func(i, j int) bool {
				if cks[i].nodeID != cks[j].nodeID {
					return cks[i].nodeID < cks[j].nodeID
				}
				return cks[i].resource < cks[j].resource
			})
			for _, ck := range cks {
				pool := a.poolOf(st, ck.nodeID, "")
				cell := foldBucket(ws.window, ck, ws.samples[ck], pool)
				cell.Version = 1
				cell.Sealed = true
				a.sealedCells[ck] = cell
				res.sealed = append(res.sealed, cell)
			}
		}
		if a.purged(ws.window) {
			// 越过补算期：清理原始采样与去重记录，已封口单元保留。
			for _, bucket := range ws.samples {
				for id := range bucket {
					delete(a.seen, id)
				}
			}
			delete(a.windows, st)
		}
	}
}

// foldBucket 用确定性顺序折叠一个单元的全部采样。
func foldBucket(w Window, ck cellKey, bucket map[string]Sample, pool string) Cell {
	ids := make([]string, 0, len(bucket))
	for id := range bucket {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	cell := Cell{
		Window:   w,
		NodeID:   ck.nodeID,
		Pool:     pool,
		Resource: ck.resource,
	}
	for i, id := range ids {
		v := bucket[id].Value
		cell.Count++
		cell.Sum += v
		if i == 0 || v < cell.Min {
			cell.Min = v
		}
		if i == 0 || v > cell.Max {
			cell.Max = v
		}
	}
	return cell
}

// snapshot 生成当前状态的不可变视图。
// 未封口窗口的单元实时计算并标记 Sealed=false。
func (a *aggregator) snapshot() *Snapshot {
	cells := make([]Cell, 0, len(a.sealedCells))
	for _, c := range a.sealedCells {
		cells = append(cells, c)
	}
	// 未封口窗口的临时单元（不写入 sealedCells）。
	starts := make([]time.Time, 0, len(a.windows))
	for st := range a.windows {
		starts = append(starts, st)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for _, st := range starts {
		ws := a.windows[st]
		if ws.sealed {
			continue
		}
		for ck, bucket := range ws.samples {
			pool := a.poolOf(st, ck.nodeID, "")
			cell := foldBucket(ws.window, ck, bucket, pool)
			cell.Sealed = false
			cells = append(cells, cell)
		}
	}
	sort.Slice(cells, func(i, j int) bool {
		if !cells[i].Window.Start.Equal(cells[j].Window.Start) {
			return cells[i].Window.Start.Before(cells[j].Window.Start)
		}
		if cells[i].NodeID != cells[j].NodeID {
			return cells[i].NodeID < cells[j].NodeID
		}
		return cells[i].Resource < cells[j].Resource
	})
	revs := make([]Revision, len(a.revisions))
	copy(revs, a.revisions)
	return &Snapshot{
		cells:     cells,
		revisions: revs,
		watermark: a.watermark.UnixNano(),
	}
}

// recover 从持久化状态恢复聚合器。
// 已清理窗口的单元直接采用快照值；未清理窗口从 WAL 采样重建，
// 其中已封口的单元用全量采样重算（版本沿用快照值），
// 保证恢复后不重复计入也不丢失。
func (a *aggregator) recover(sf snapshotFile, walSamples []Sample) {
	a.watermark = timeFromNano(sf.Watermark)
	a.maxEvent = timeFromNano(sf.MaxEvent)
	for _, c := range sf.SealedCells {
		a.sealedCells[cellKey{windowStart: c.Window.Start, nodeID: c.NodeID, resource: c.Resource}] = c
	}
	a.revisions = append(a.revisions, sf.Revisions...)

	// 重放 WAL：重建未清理窗口的原始采样。
	for _, s := range walSamples {
		wins := a.assigner.assign(s.Timestamp)
		kept := false
		for _, w := range wins {
			if a.purged(w) {
				continue
			}
			kept = true
			ws := a.windows[w.Start]
			if ws == nil {
				ws = &windowState{window: w, samples: make(map[cellKey]map[string]Sample)}
				a.windows[w.Start] = ws
			}
			ck := cellKey{windowStart: w.Start, nodeID: s.NodeID, resource: s.Resource}
			bucket := ws.samples[ck]
			if bucket == nil {
				bucket = make(map[string]Sample)
				ws.samples[ck] = bucket
			}
			bucket[s.SampleID] = s
		}
		if kept {
			a.seen[s.SampleID] = s.Timestamp.UnixNano()
		}
	}

	// 标记已封口窗口，并用全量采样重算其单元（版本沿用快照值）。
	for st, ws := range a.windows {
		sealed := false
		for ck := range ws.samples {
			if _, ok := a.sealedCells[ck]; ok {
				sealed = true
				break
			}
		}
		if !sealed {
			continue
		}
		ws.sealed = true
		for ck, bucket := range ws.samples {
			prev, ok := a.sealedCells[ck]
			if !ok {
				continue
			}
			cell := foldBucket(ws.window, ck, bucket, a.poolOf(st, ck.nodeID, prev.Pool))
			cell.Version = prev.Version
			cell.Sealed = true
			a.sealedCells[ck] = cell
		}
	}

	// 崩溃可能发生在 WAL 落盘与封口之间：补一次封口与清理。
	var res ingestResult
	a.sealAndPurge(&res)
}

// sealedSnapshot 导出当前持久化快照内容。
func (a *aggregator) sealedSnapshot() snapshotFile {
	cells := make([]Cell, 0, len(a.sealedCells))
	for _, c := range a.sealedCells {
		cells = append(cells, c)
	}
	revs := make([]Revision, len(a.revisions))
	copy(revs, a.revisions)
	return snapshotFile{
		Watermark:   a.watermark.UnixNano(),
		MaxEvent:    a.maxEvent.UnixNano(),
		SealedCells: cells,
		Revisions:   revs,
	}
}

// checkpointSamples 返回需保留在 WAL 中的采样（未清理窗口的全部采样），
// 顺序确定（按窗口起点 + SampleID 排序）。
func (a *aggregator) checkpointSamples() []Sample {
	var out []Sample
	starts := make([]time.Time, 0, len(a.windows))
	for st := range a.windows {
		starts = append(starts, st)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for _, st := range starts {
		ws := a.windows[st]
		byID := make(map[string]Sample)
		for _, bucket := range ws.samples {
			for id, s := range bucket {
				byID[id] = s
			}
		}
		ids := make([]string, 0, len(byID))
		for id := range byID {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			out = append(out, byID[id])
		}
	}
	return out
}
