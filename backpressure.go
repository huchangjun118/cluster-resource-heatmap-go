package heatmap

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// ErrQueueFull is returned when the bounded queue is full and the policy is drop.
var ErrQueueFull = errors.New("sample queue full")

// DropPolicy determines behavior when the bounded queue is full.
type DropPolicy int

const (
	// DropPolicyBlock blocks the producer until space is available (backpressure).
	DropPolicyBlock DropPolicy = iota
	// DropPolicyDrop drops the sample and records a metric.
	DropPolicyDrop
)

// BoundedQueue is a bounded, in-memory sample queue with a configurable full policy.
type BoundedQueue struct {
	ch      chan Sample
	policy  DropPolicy
	metrics *Metrics
}

// NewBoundedQueue constructs a queue with the given capacity and policy.
func NewBoundedQueue(capacity int, policy DropPolicy, metrics *Metrics) *BoundedQueue {
	if capacity <= 0 {
		capacity = 1024
	}
	if metrics == nil {
		metrics = &Metrics{}
	}
	return &BoundedQueue{
		ch:      make(chan Sample, capacity),
		policy:  policy,
		metrics: metrics,
	}
}

// Submit enqueues a sample, honoring the full policy.
func (q *BoundedQueue) Submit(s Sample) error {
	return q.SubmitContext(context.Background(), s)
}

// SubmitContext enqueues a sample, aborting if ctx is canceled.
func (q *BoundedQueue) SubmitContext(ctx context.Context, s Sample) error {
	select {
	case q.ch <- s:
		q.metrics.QueueDepth.Store(int64(len(q.ch)))
		return nil
	default:
		// Queue is full.
		if q.policy == DropPolicyDrop {
			q.metrics.SamplesDropped.Add(1)
			return ErrQueueFull
		}
		// Block policy: wait for space or cancellation.
		select {
		case q.ch <- s:
			q.metrics.QueueDepth.Store(int64(len(q.ch)))
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Next returns the receive channel for the consumer.
func (q *BoundedQueue) Next() <-chan Sample {
	return q.ch
}

// Close closes the queue.
func (q *BoundedQueue) Close() {
	close(q.ch)
}

// Depth returns the current queue depth.
func (q *BoundedQueue) Depth() int64 {
	return int64(len(q.ch))
}

// simulateProcessingDelay is a hook for testing slow consumers.
var simulateProcessingDelay atomic.Int64

// processingDelay returns the current processing delay.
func processingDelay() time.Duration {
	return time.Duration(simulateProcessingDelay.Load())
}

// SetSimulateProcessingDelay sets the simulated processing delay (for testing).
func SetSimulateProcessingDelay(d time.Duration) {
	simulateProcessingDelay.Store(int64(d))
}
