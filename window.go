package heatmap

import (
	"errors"
	"time"
)

// WindowAssigner maps event times to deterministic window boundaries.
//
// Tumbling windows (Slide == Size) assign each event to exactly one window.
// Sliding windows (Slide < Size) assign each event to multiple overlapping
// windows. Boundaries are computed from a fixed origin so the same event time
// always maps to the same windows regardless of arrival order or concurrency.
type WindowAssigner struct {
	Size   time.Duration
	Slide  time.Duration
	origin time.Time
}

// NewWindowAssigner constructs an assigner. Slide must be > 0 and <= Size,
// and Size must be a positive multiple of Slide.
func NewWindowAssigner(size, slide time.Duration) (*WindowAssigner, error) {
	if size <= 0 {
		return nil, errors.New("window size must be positive")
	}
	if slide <= 0 {
		return nil, errors.New("window slide must be positive")
	}
	if slide > size {
		return nil, errors.New("window slide must not exceed size")
	}
	if size%slide != 0 {
		return nil, errors.New("window size must be a multiple of slide")
	}
	return &WindowAssigner{Size: size, Slide: slide, origin: time.Unix(0, 0)}, nil
}

// WindowsFor returns all windows containing t, ordered by ascending start.
func (a *WindowAssigner) WindowsFor(t time.Time) []Window {
	delta := t.Sub(a.origin)
	kMax := floorDiv(delta, a.Slide)
	var out []Window
	for k := kMax; ; k-- {
		startNano := a.origin.UnixNano() + k*int64(a.Slide)
		start := time.Unix(0, startNano)
		if !start.Add(a.Size).After(t) {
			break
		}
		out = append(out, Window{Start: start, End: start.Add(a.Size)})
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// WindowStart returns the start of the (single) window containing t.
// For sliding windows it returns the most recent window start <= t.
func (a *WindowAssigner) WindowStart(t time.Time) time.Time {
	delta := t.Sub(a.origin)
	k := floorDiv(delta, a.Slide)
	startNano := a.origin.UnixNano() + k*int64(a.Slide)
	return time.Unix(0, startNano)
}

// IsTumbling reports whether the assigner produces tumbling windows.
func (a *WindowAssigner) IsTumbling() bool {
	return a.Slide == a.Size
}

// NumWindows returns the number of windows an event is assigned to.
func (a *WindowAssigner) NumWindows() int {
	return int(a.Size / a.Slide)
}

// floorDiv returns floor(a / b) for positive b.
func floorDiv(a, b time.Duration) int64 {
	q := a / b
	r := a % b
	if r != 0 && a < 0 {
		q--
	}
	return int64(q)
}
