package heatmap

import (
	"sync"
	"time"
)

// Revision records a single change to a sealed heatmap cell.
type Revision struct {
	WindowStart int64
	NodeID      string
	Resource    ResourceType
	OldVersion  int64
	NewVersion  int64
	Trigger     SampleKey
	Timestamp   time.Time
}

// RevisionLog is an append-only, versioned record of all cell revisions.
type RevisionLog struct {
	mu        sync.Mutex
	revisions []Revision
	versions  map[CellKey]int64
}

// NewRevisionLog constructs an empty revision log.
func NewRevisionLog() *RevisionLog {
	return &RevisionLog{versions: make(map[CellKey]int64)}
}

// CurrentVersion returns the current version for a cell (0 if never revised).
func (l *RevisionLog) CurrentVersion(key CellKey) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.versions[key]
}

// Append records a revision and updates the cell's current version.
func (l *RevisionLog) Append(rev Revision) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.revisions = append(l.revisions, rev)
	key := CellKey{WindowStart: rev.WindowStart, NodeID: rev.NodeID, Resource: rev.Resource}
	l.versions[key] = rev.NewVersion
}

// All returns a copy of all revisions in order.
func (l *RevisionLog) All() []Revision {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Revision, len(l.revisions))
	copy(out, l.revisions)
	return out
}

// Len returns the number of revisions.
func (l *RevisionLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.revisions)
}

// Snapshot returns a copy of the version map.
func (l *RevisionLog) Snapshot() map[CellKey]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[CellKey]int64, len(l.versions))
	for k, v := range l.versions {
		out[k] = v
	}
	return out
}

// Restore replaces the log contents (used in recovery).
func (l *RevisionLog) Restore(revs []Revision, versions map[CellKey]int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.revisions = append([]Revision(nil), revs...)
	l.versions = make(map[CellKey]int64, len(versions))
	for k, v := range versions {
		l.versions[k] = v
	}
}
