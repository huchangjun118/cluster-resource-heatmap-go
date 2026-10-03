package heatmap

import "time"

// WatermarkTracker tracks the event-time watermark: the observed maximum event
// time minus the allowed lateness. Windows whose end is <= watermark are sealed.
type WatermarkTracker struct {
	allowedLateness time.Duration
	maxEventTime    time.Time
	hasData         bool
}

// NewWatermarkTracker constructs a tracker with the given allowed lateness.
func NewWatermarkTracker(allowedLateness time.Duration) *WatermarkTracker {
	return &WatermarkTracker{allowedLateness: allowedLateness}
}

// Observe records an event time and returns the current watermark and whether
// it advanced. The watermark is monotonically non-decreasing.
func (w *WatermarkTracker) Observe(t time.Time) (watermark time.Time, advanced bool) {
	if !w.hasData || t.After(w.maxEventTime) {
		w.maxEventTime = t
		w.hasData = true
	}
	wm := w.Watermark()
	return wm, false
}

// Watermark returns the current watermark (maxEventTime - allowedLateness).
func (w *WatermarkTracker) Watermark() time.Time {
	if !w.hasData {
		return time.Time{}
	}
	return w.maxEventTime.Add(-w.allowedLateness)
}

// HasData reports whether any event time has been observed.
func (w *WatermarkTracker) HasData() bool {
	return w.hasData
}

// IsSealed reports whether a window ending at windowEnd should be sealed given
// the current watermark.
func (w *WatermarkTracker) IsSealed(windowEnd time.Time) bool {
	if !w.hasData {
		return false
	}
	return !windowEnd.After(w.Watermark())
}

// MaxEventTime returns the maximum observed event time.
func (w *WatermarkTracker) MaxEventTime() time.Time {
	return w.maxEventTime
}

// SetMaxEventTime restores the tracker state (used in recovery).
func (w *WatermarkTracker) SetMaxEventTime(t time.Time) {
	w.maxEventTime = t
	w.hasData = true
}
