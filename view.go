package heatmap

import (
	"sort"
	"time"
)

// View is an immutable, committed snapshot of the heatmap. Queries read a View
// without blocking aggregation; the aggregator publishes new views atomically.
type View struct {
	cells       map[int64]map[CellKey]Cell
	windowOrder []int64
	size        time.Duration
	watermark   time.Time
	committedAt time.Time
}

// newView constructs a view from the given cell map. The map is copied so that
// later mutation of the aggregator's state cannot affect the view.
func newView(cells map[int64]map[CellKey]Cell, size time.Duration, watermark time.Time, committedAt time.Time) *View {
	v := &View{
		cells:       make(map[int64]map[CellKey]Cell, len(cells)),
		size:        size,
		watermark:   watermark,
		committedAt: committedAt,
	}
	for ws, m := range cells {
		cp := make(map[CellKey]Cell, len(m))
		for k, c := range m {
			cp[k] = c
		}
		v.cells[ws] = cp
		v.windowOrder = append(v.windowOrder, ws)
	}
	sort.Slice(v.windowOrder, func(i, j int) bool {
		return v.windowOrder[i] < v.windowOrder[j]
	})
	return v
}

// CellAt returns the cell for the given key, if present.
func (v *View) CellAt(key CellKey) (Cell, bool) {
	m, ok := v.cells[key.WindowStart]
	if !ok {
		return Cell{}, false
	}
	c, ok := m[key]
	return c, ok
}

// WindowStarts returns all window starts in the view, ascending.
func (v *View) WindowStarts() []int64 {
	out := make([]int64, len(v.windowOrder))
	copy(out, v.windowOrder)
	return out
}

// Watermark returns the watermark at commit time.
func (v *View) Watermark() time.Time {
	return v.watermark
}

// CommittedAt returns the commit timestamp.
func (v *View) CommittedAt() time.Time {
	return v.committedAt
}

// Size returns the window size.
func (v *View) Size() time.Duration {
	return v.size
}

// windowsInRange returns the window starts whose windows overlap [start, end).
// A window [w, w+size) overlaps [start, end) iff w < end and w+size > start.
func (v *View) windowsInRange(start, end time.Time) []int64 {
	var starts []int64
	for _, ws := range v.windowOrder {
		w := time.Unix(0, ws)
		win := Window{Start: w, End: w.Add(v.size)}
		if windowInRange(win, start, end) {
			starts = append(starts, ws)
		}
	}
	return starts
}

// cellsByWindow returns the cells for the given window starts, keyed by window start.
func (v *View) cellsByWindow(windows []int64) map[int64][]Cell {
	out := make(map[int64][]Cell, len(windows))
	for _, ws := range windows {
		m := v.cells[ws]
		for _, c := range m {
			out[ws] = append(out[ws], c)
		}
	}
	return out
}

// mergeCells combines cells with the same (NodeID, Resource) into one per key.
// Used for within-window drill-downs (e.g. pool from nodes). Each input cell is
// merged exactly once, so no sample is double-counted or lost.
func mergeCells(cells []Cell) []Cell {
	type mergeKey struct {
		NodeID   string
		Resource ResourceType
	}
	acc := make(map[mergeKey]*Cell)
	for _, c := range cells {
		k := mergeKey{NodeID: c.NodeID, Resource: c.Resource}
		if existing, ok := acc[k]; ok {
			merged := existing.Merge(c)
			*existing = merged
		} else {
			cp := c
			acc[k] = &cp
		}
	}
	out := make([]Cell, 0, len(acc))
	for _, c := range acc {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeID != out[j].NodeID {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].Resource < out[j].Resource
	})
	return out
}
