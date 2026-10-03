package heatmap

import "time"

// ResourceType identifies the kind of resource being sampled.
type ResourceType string

const (
	ResourceCPU    ResourceType = "cpu"
	ResourceMemory ResourceType = "memory"
	ResourceDisk   ResourceType = "disk"
	ResourceNetIn  ResourceType = "net_in"
	ResourceNetOut ResourceType = "net_out"
)

// AllResources returns the known resource types in a stable order.
func AllResources() []ResourceType {
	return []ResourceType{
		ResourceCPU, ResourceMemory, ResourceDisk, ResourceNetIn, ResourceNetOut,
	}
}

// Sample is a single resource reading reported by a node.
type Sample struct {
	NodeID    string
	PoolID    string
	Resource  ResourceType
	Value     float64
	EventTime time.Time
	Seq       uint64
}

// SampleKey is the identity used for duplicate detection.
type SampleKey struct {
	NodeID string
	Seq    uint64
}

// Key returns the deduplication identity of the sample.
func (s Sample) Key() SampleKey {
	return SampleKey{NodeID: s.NodeID, Seq: s.Seq}
}

// Window is a half-open time interval [Start, End).
type Window struct {
	Start time.Time
	End   time.Time
}

// Contains reports whether t falls inside the window.
func (w Window) Contains(t time.Time) bool {
	return !t.Before(w.Start) && t.Before(w.End)
}

// CellKey identifies a single heatmap cell within a window.
type CellKey struct {
	WindowStart int64 // unix nanoseconds
	NodeID      string
	Resource    ResourceType
}

// Cell is an aggregated heatmap unit for one (window, node, resource).
type Cell struct {
	Window   Window
	NodeID   string
	PoolID   string
	Resource ResourceType
	Count    int64
	Sum      float64
	Min      float64
	Max      float64
	Version  int64
}

// Merge combines two cells covering the same (window, node, resource) into one.
// The result is identical to aggregating the union of raw samples, because the
// summary statistics are distributive: count and sum add, min takes the smaller,
// max takes the larger. The Version field is left to the caller to set.
func (c Cell) Merge(o Cell) Cell {
	if c.Count == 0 {
		return o
	}
	if o.Count == 0 {
		return c
	}
	res := Cell{
		Window:   c.Window,
		NodeID:   c.NodeID,
		PoolID:   c.PoolID,
		Resource: c.Resource,
		Count:    c.Count + o.Count,
		Sum:      c.Sum + o.Sum,
	}
	if c.Min < o.Min {
		res.Min = c.Min
	} else {
		res.Min = o.Min
	}
	if c.Max > o.Max {
		res.Max = c.Max
	} else {
		res.Max = o.Max
	}
	return res
}
