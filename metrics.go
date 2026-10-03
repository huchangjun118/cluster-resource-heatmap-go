package heatmap

import "sync/atomic"

// Metrics collects observable counters for the service.
type Metrics struct {
	SamplesIngested atomic.Uint64
	SamplesDropped  atomic.Uint64
	Duplicates      atomic.Uint64
	LateRevisions   atomic.Uint64
	WindowsSealed   atomic.Uint64
	Revisions       atomic.Uint64
	ForcedSeals     atomic.Uint64
	QueueDepth      atomic.Int64
	OpenWindows     atomic.Int64
}

// Snapshot is a point-in-time copy of metrics.
type Snapshot struct {
	SamplesIngested uint64
	SamplesDropped  uint64
	Duplicates      uint64
	LateRevisions   uint64
	WindowsSealed   uint64
	Revisions       uint64
	ForcedSeals     uint64
	QueueDepth      int64
	OpenWindows     int64
}

// Snapshot returns a consistent point-in-time copy.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		SamplesIngested: m.SamplesIngested.Load(),
		SamplesDropped:  m.SamplesDropped.Load(),
		Duplicates:      m.Duplicates.Load(),
		LateRevisions:   m.LateRevisions.Load(),
		WindowsSealed:   m.WindowsSealed.Load(),
		Revisions:       m.Revisions.Load(),
		ForcedSeals:     m.ForcedSeals.Load(),
		QueueDepth:      m.QueueDepth.Load(),
		OpenWindows:     m.OpenWindows.Load(),
	}
}
