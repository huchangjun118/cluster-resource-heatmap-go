package heatmap

import (
	"sort"
	"sync"
	"time"
)

// accumulator is the in-memory aggregation state for an open cell.
type accumulator struct {
	count int64
	sum   float64
	min   float64
	max   float64
}

func (a *accumulator) add(v float64) {
	a.count++
	a.sum += v
	if a.count == 1 {
		a.min = v
		a.max = v
	} else {
		if v < a.min {
			a.min = v
		}
		if v > a.max {
			a.max = v
		}
	}
}

func (a *accumulator) toCell(w Window, nodeID, poolID string, resource ResourceType, version int64) Cell {
	return Cell{
		Window: w, NodeID: nodeID, PoolID: poolID, Resource: resource,
		Count: a.count, Sum: a.sum, Min: a.min, Max: a.max, Version: version,
	}
}

// Aggregator ingests samples, maintains open windows, seals them based on the
// watermark, and revises sealed windows when late data arrives.
type Aggregator struct {
	mu          sync.Mutex
	assigner    *WindowAssigner
	watermark   *WatermarkTracker
	allowedLate time.Duration

	// open windows: windowStart -> CellKey -> accumulator
	open map[int64]map[CellKey]*accumulator
	// sealed cells: windowStart -> CellKey -> Cell
	sealed map[int64]map[CellKey]Cell
	// nodePool maps nodeID to its poolID
	nodePool map[string]string
	// seen sample keys for duplicate detection, with event time for eviction
	seen map[SampleKey]time.Time

	revisions *RevisionLog
	view      *View
	metrics   *Metrics
	wal       *WAL

	maxOpenWindows int
	maxSeen        int
}

// AggregatorConfig holds aggregator parameters.
type AggregatorConfig struct {
	WindowSize      time.Duration
	WindowSlide     time.Duration
	AllowedLateness time.Duration
	MaxOpenWindows  int
	MaxSeen         int
}

// NewAggregator constructs an aggregator.
func NewAggregator(cfg AggregatorConfig, wal *WAL, metrics *Metrics) (*Aggregator, error) {
	assigner, err := NewWindowAssigner(cfg.WindowSize, cfg.WindowSlide)
	if err != nil {
		return nil, err
	}
	if metrics == nil {
		metrics = &Metrics{}
	}
	maxOpen := cfg.MaxOpenWindows
	if maxOpen <= 0 {
		maxOpen = 1000
	}
	maxSeen := cfg.MaxSeen
	if maxSeen <= 0 {
		maxSeen = 100000
	}
	a := &Aggregator{
		assigner:       assigner,
		watermark:      NewWatermarkTracker(cfg.AllowedLateness),
		allowedLate:    cfg.AllowedLateness,
		open:           make(map[int64]map[CellKey]*accumulator),
		sealed:         make(map[int64]map[CellKey]Cell),
		nodePool:       make(map[string]string),
		seen:           make(map[SampleKey]time.Time),
		revisions:      NewRevisionLog(),
		metrics:        metrics,
		wal:            wal,
		maxOpenWindows: maxOpen,
		maxSeen:        maxSeen,
	}
	a.commitViewLocked()
	return a, nil
}

// Process ingests one sample. It is safe for concurrent use.
func (a *Aggregator) Process(s Sample) error {
	return a.process(s, true)
}

// ProcessReplay ingests a sample during recovery without writing to the WAL.
func (a *Aggregator) ProcessReplay(s Sample) error {
	return a.process(s, false)
}

func (a *Aggregator) process(s Sample, writeWAL bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := s.Key()
	if _, ok := a.seen[key]; ok {
		a.metrics.Duplicates.Add(1)
		return nil
	}
	a.seen[key] = s.EventTime
	a.evictSeenLocked()

	if writeWAL && a.wal != nil {
		if err := a.wal.Append(s); err != nil {
			return err
		}
	}

	a.metrics.SamplesIngested.Add(1)
	if s.PoolID != "" {
		a.nodePool[s.NodeID] = s.PoolID
	}
	a.watermark.Observe(s.EventTime)

	changed := false
	windows := a.assigner.WindowsFor(s.EventTime)
	for _, w := range windows {
		ws := w.Start.UnixNano()
		if a.watermark.IsSealed(w.End) {
			a.reviseLocked(s, w)
			changed = true
		} else {
			a.addToOpenLocked(s, ws)
		}
	}

	if a.sealReadyLocked() {
		changed = true
	}

	if a.maxOpenWindows > 0 && len(a.open) > a.maxOpenWindows {
		a.forceSealOldestLocked()
		changed = true
	}

	if changed {
		a.commitViewLocked()
	}

	return nil
}

// addToOpenLocked adds a sample to the open accumulator for a window.
// Caller must hold mu.
func (a *Aggregator) addToOpenLocked(s Sample, ws int64) {
	accs, ok := a.open[ws]
	if !ok {
		accs = make(map[CellKey]*accumulator)
		a.open[ws] = accs
	}
	ck := CellKey{WindowStart: ws, NodeID: s.NodeID, Resource: s.Resource}
	acc, ok := accs[ck]
	if !ok {
		acc = &accumulator{}
		accs[ck] = acc
	}
	acc.add(s.Value)
}

// sealReadyLocked seals all open windows whose end is <= watermark.
// Returns true if any window was sealed. Caller must hold mu.
func (a *Aggregator) sealReadyLocked() bool {
	var toSeal []int64
	for ws := range a.open {
		w := time.Unix(0, ws)
		win := Window{Start: w, End: w.Add(a.assigner.Size)}
		if a.watermark.IsSealed(win.End) {
			toSeal = append(toSeal, ws)
		}
	}
	for _, ws := range toSeal {
		a.sealWindowLocked(ws)
	}
	return len(toSeal) > 0
}

// sealWindowLocked seals a single open window, producing sealed cells.
// Caller must hold mu.
func (a *Aggregator) sealWindowLocked(ws int64) {
	accs, ok := a.open[ws]
	if !ok {
		return
	}
	w := time.Unix(0, ws)
	win := Window{Start: w, End: w.Add(a.assigner.Size)}
	cells := make(map[CellKey]Cell, len(accs))
	for ck, acc := range accs {
		poolID := a.nodePool[ck.NodeID]
		version := a.revisions.CurrentVersion(ck)
		cells[ck] = acc.toCell(win, ck.NodeID, poolID, ck.Resource, version)
	}
	a.sealed[ws] = cells
	delete(a.open, ws)
	a.metrics.WindowsSealed.Add(1)
	a.metrics.OpenWindows.Store(int64(len(a.open)))
}

// reviseLocked applies a late sample to an already-sealed window.
// Caller must hold mu.
func (a *Aggregator) reviseLocked(s Sample, w Window) {
	ws := w.Start.UnixNano()
	ck := CellKey{WindowStart: ws, NodeID: s.NodeID, Resource: s.Resource}

	existing := a.sealed[ws][ck]
	if existing.Count == 0 {
		existing = Cell{Window: w, NodeID: s.NodeID, PoolID: s.PoolID, Resource: s.Resource}
	}

	contribution := Cell{
		Window: w, NodeID: s.NodeID, PoolID: s.PoolID, Resource: s.Resource,
		Count: 1, Sum: s.Value, Min: s.Value, Max: s.Value,
	}

	merged := existing.Merge(contribution)
	oldVersion := existing.Version
	newVersion := oldVersion + 1
	merged.Version = newVersion

	if a.sealed[ws] == nil {
		a.sealed[ws] = make(map[CellKey]Cell)
	}
	a.sealed[ws][ck] = merged

	a.revisions.Append(Revision{
		WindowStart: ws, NodeID: s.NodeID, Resource: s.Resource,
		OldVersion: oldVersion, NewVersion: newVersion,
		Trigger: s.Key(), Timestamp: Now(),
	})

	a.metrics.LateRevisions.Add(1)
	a.metrics.Revisions.Add(1)
}

// forceSealOldestLocked seals the oldest open windows until within the limit.
// Caller must hold mu.
func (a *Aggregator) forceSealOldestLocked() {
	var starts []int64
	for ws := range a.open {
		starts = append(starts, ws)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	for len(a.open) > a.maxOpenWindows && len(starts) > 0 {
		ws := starts[0]
		starts = starts[1:]
		a.sealWindowLocked(ws)
		a.metrics.ForcedSeals.Add(1)
	}
}

// commitViewLocked builds and publishes a new view from the sealed state.
// Caller must hold mu.
func (a *Aggregator) commitViewLocked() {
	view := newView(a.sealed, a.assigner.Size, a.watermark.Watermark(), Now())
	a.view = view
}

// evictSeenLocked removes old seen entries to bound memory. Caller must hold mu.
func (a *Aggregator) evictSeenLocked() {
	if len(a.seen) < a.maxSeen {
		return
	}
	cutoff := a.watermark.Watermark().Add(-a.allowedLate)
	for k, t := range a.seen {
		if t.Before(cutoff) {
			delete(a.seen, k)
		}
	}
}

// View returns the current committed view.
func (a *Aggregator) View() *View {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.view
}

// Metrics returns a copy of the metrics snapshot.
func (a *Aggregator) Metrics() Snapshot {
	return a.metrics.Snapshot()
}

// Revisions returns all recorded revisions.
func (a *Aggregator) Revisions() []Revision {
	return a.revisions.All()
}

// RestoreSealed restores sealed cells from a snapshot (recovery).
func (a *Aggregator) RestoreSealed(sealed map[int64]map[CellKey]Cell) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sealed = make(map[int64]map[CellKey]Cell, len(sealed))
	for ws, m := range sealed {
		cp := make(map[CellKey]Cell, len(m))
		for k, c := range m {
			cp[k] = c
			a.nodePool[c.NodeID] = c.PoolID
		}
		a.sealed[ws] = cp
	}
	a.commitViewLocked()
}

// RestoreWatermark restores the watermark from a snapshot (recovery).
func (a *Aggregator) RestoreWatermark(maxEventTime time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.watermark.SetMaxEventTime(maxEventTime)
}

// RestoreRevisions restores the revision log from a snapshot (recovery).
func (a *Aggregator) RestoreRevisions(revs []Revision) {
	a.mu.Lock()
	defer a.mu.Unlock()
	versions := make(map[CellKey]int64)
	for _, r := range revs {
		versions[CellKey{WindowStart: r.WindowStart, NodeID: r.NodeID, Resource: r.Resource}] = r.NewVersion
	}
	a.revisions.Restore(revs, versions)
}

// SeenCount returns the number of tracked sample identities.
func (a *Aggregator) SeenCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.seen)
}

// SnapshotState exports the full sealed state for persistence.
func (a *Aggregator) SnapshotState() StateSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	sealed := make(map[int64]map[CellKey]Cell, len(a.sealed))
	for ws, m := range a.sealed {
		cp := make(map[CellKey]Cell, len(m))
		for k, c := range m {
			cp[k] = c
		}
		sealed[ws] = cp
	}
	revs := a.revisions.All()
	seen := make(map[SampleKey]time.Time, len(a.seen))
	for k, t := range a.seen {
		seen[k] = t
	}
	return StateSnapshot{
		Sealed:      sealed,
		Revisions:   revs,
		Watermark:   a.watermark.MaxEventTime(),
		Seen:        seen,
		CommittedAt: Now(),
	}
}

// RestoreSeen restores the seen set from a snapshot (recovery).
func (a *Aggregator) RestoreSeen(seen map[SampleKey]time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen = make(map[SampleKey]time.Time, len(seen))
	for k, t := range seen {
		a.seen[k] = t
	}
}
