package heatmap

import (
	"context"
	"os"
	"sync"
	"time"
)

// Service is the cluster resource heatmap data service.
type Service struct {
	cfg     Config
	queue   *BoundedQueue
	agg     *Aggregator
	wal     *WAL
	metrics *Metrics

	sampleWg sync.WaitGroup
	wg       sync.WaitGroup
	stopCh   chan struct{}
}

// NewService constructs a service from configuration.
func NewService(cfg Config) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	metrics := &Metrics{}
	queue := NewBoundedQueue(cfg.QueueCapacity, cfg.DropPolicy, metrics)
	var wal *WAL
	if cfg.WALPath != "" {
		var err error
		wal, err = NewWAL(cfg.WALPath)
		if err != nil {
			return nil, err
		}
	}
	agg, err := NewAggregator(AggregatorConfig{
		WindowSize:      cfg.WindowSize,
		WindowSlide:     cfg.WindowSlide,
		AllowedLateness: cfg.AllowedLateness,
		MaxOpenWindows:  cfg.MaxOpenWindows,
		MaxSeen:         cfg.MaxSeen,
	}, wal, metrics)
	if err != nil {
		return nil, err
	}
	s := &Service{
		cfg:     cfg,
		queue:   queue,
		agg:     agg,
		wal:     wal,
		metrics: metrics,
		stopCh:  make(chan struct{}),
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

// Start begins the aggregation loop.
func (s *Service) Start() error {
	s.wg.Add(1)
	go s.run()
	return nil
}

func (s *Service) run() {
	defer s.wg.Done()
	for {
		select {
		case sample, ok := <-s.queue.Next():
			if !ok {
				return
			}
			if d := processingDelay(); d > 0 {
				time.Sleep(d)
			}
			if err := s.agg.Process(sample); err != nil {
				// WAL write error; continue to avoid stalling the pipeline.
				_ = err
			}
			s.sampleWg.Done()
			s.metrics.QueueDepth.Store(s.queue.Depth())
		case <-s.stopCh:
			return
		}
	}
}

// Stop gracefully stops the service.
func (s *Service) Stop() error {
	close(s.stopCh)
	s.wg.Wait()
	s.queue.Close()
	if s.wal != nil {
		s.wal.Close()
	}
	return nil
}

// Submit enqueues a sample for processing.
func (s *Service) Submit(sample Sample) error {
	s.sampleWg.Add(1)
	if err := s.queue.Submit(sample); err != nil {
		s.sampleWg.Done()
		return err
	}
	return nil
}

// SubmitContext enqueues a sample, aborting on cancellation.
func (s *Service) SubmitContext(ctx context.Context, sample Sample) error {
	s.sampleWg.Add(1)
	if err := s.queue.SubmitContext(ctx, sample); err != nil {
		s.sampleWg.Done()
		return err
	}
	return nil
}

// View returns the current committed heatmap view.
func (s *Service) View() *View {
	return s.agg.View()
}

// Metrics returns the metrics snapshot.
func (s *Service) Metrics() Snapshot {
	return s.agg.Metrics()
}

// Revisions returns all recorded revisions.
func (s *Service) Revisions() []Revision {
	return s.agg.Revisions()
}

// Queue returns the bounded queue for direct source injection.
func (s *Service) Queue() *BoundedQueue {
	return s.queue
}

// Flush waits until all currently queued samples are processed.
func (s *Service) Flush(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.sampleWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Snapshot writes a full snapshot to disk.
func (s *Service) Snapshot() error {
	if s.cfg.SnapshotPath == "" {
		return nil
	}
	state := s.agg.SnapshotState()
	if s.wal != nil {
		offset, err := s.wal.Offset()
		if err != nil {
			return err
		}
		state.WALOffset = offset
	}
	return SaveSnapshot(s.cfg.SnapshotPath, state)
}

// recover restores state from a snapshot and replays the WAL.
func (s *Service) recover() error {
	var walOffset int64
	if s.cfg.SnapshotPath != "" {
		snap, err := LoadSnapshot(s.cfg.SnapshotPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			// No snapshot file; replay WAL from the beginning.
		} else {
			s.agg.RestoreSealed(snap.Sealed)
			s.agg.RestoreWatermark(snap.Watermark)
			s.agg.RestoreRevisions(snap.Revisions)
			s.agg.RestoreSeen(snap.Seen)
			walOffset = snap.WALOffset
		}
	}
	if s.wal != nil {
		if err := s.wal.Replay(walOffset, func(sample Sample) error {
			return s.agg.ProcessReplay(sample)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Now returns the current time (overridable in tests).
var Now = time.Now
