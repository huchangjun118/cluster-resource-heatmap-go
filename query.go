package heatmap

import (
	"sort"
	"time"
)

// Query describes a drill-down query over a time range.
type Query struct {
	NodeID   string
	PoolID   string
	Resource ResourceType
	Start    time.Time
	End      time.Time
}

// Heatmap returns all cells in the time range, one per (window, node, resource).
// Results are grouped by window start (ascending), then by node/resource.
func (v *View) Heatmap(start, end time.Time) []Cell {
	windows := v.windowsInRange(start, end)
	var out []Cell
	for _, ws := range windows {
		cells := v.cellsByWindow([]int64{ws})[ws]
		sort.Slice(cells, func(i, j int) bool {
			if cells[i].NodeID != cells[j].NodeID {
				return cells[i].NodeID < cells[j].NodeID
			}
			return cells[i].Resource < cells[j].Resource
		})
		out = append(out, cells...)
	}
	return out
}

// QueryByNode returns cells for a specific node across the time range, per window.
func (v *View) QueryByNode(nodeID string, start, end time.Time) []Cell {
	windows := v.windowsInRange(start, end)
	var out []Cell
	for _, ws := range windows {
		for _, c := range v.cells[ws] {
			if c.NodeID == nodeID {
				out = append(out, c)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Window.Start.Before(out[j].Window.Start)
	})
	return out
}

// QueryByPool returns aggregated cells for a pool across the time range.
// Within each window, node-cells of the pool are merged into one pool-cell.
// No sample is double-counted because each event belongs to exactly one node
// per window, and merging is distributive.
func (v *View) QueryByPool(poolID string, start, end time.Time) []Cell {
	windows := v.windowsInRange(start, end)
	var out []Cell
	for _, ws := range windows {
		var cells []Cell
		for _, c := range v.cells[ws] {
			if c.PoolID == poolID {
				cells = append(cells, c)
			}
		}
		merged := mergeCells(cells)
		out = append(out, merged...)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Window.Start.Before(out[j].Window.Start)
	})
	return out
}

// QueryByResource returns cells for a resource type across the range, per window.
func (v *View) QueryByResource(resource ResourceType, start, end time.Time) []Cell {
	windows := v.windowsInRange(start, end)
	var out []Cell
	for _, ws := range windows {
		for _, c := range v.cells[ws] {
			if c.Resource == resource {
				out = append(out, c)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Window.Start.Before(out[j].Window.Start)
	})
	return out
}

// QueryCells runs a generic query and returns matching cells.
func (v *View) QueryCells(q Query) []Cell {
	windows := v.windowsInRange(q.Start, q.End)
	var out []Cell
	for _, ws := range windows {
		var cells []Cell
		for _, c := range v.cells[ws] {
			if q.NodeID != "" && c.NodeID != q.NodeID {
				continue
			}
			if q.PoolID != "" && c.PoolID != q.PoolID {
				continue
			}
			if q.Resource != "" && c.Resource != q.Resource {
				continue
			}
			cells = append(cells, c)
		}
		if q.PoolID != "" {
			cells = mergeCells(cells)
		}
		out = append(out, cells...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Window.Start.Equal(out[j].Window.Start) {
			if out[i].NodeID != out[j].NodeID {
				return out[i].NodeID < out[j].NodeID
			}
			return out[i].Resource < out[j].Resource
		}
		return out[i].Window.Start.Before(out[j].Window.Start)
	})
	return out
}

// windowInRange reports whether a window overlaps [start, end).
func windowInRange(w Window, start, end time.Time) bool {
	return w.Start.Before(end) && w.End.After(start)
}
