package heatmap

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// Submitter is the interface for submitting samples to the service.
type Submitter interface {
	Submit(sample Sample) error
	SubmitContext(ctx context.Context, sample Sample) error
}

// SourceConfig configures the simulated sample source.
type SourceConfig struct {
	NumNodes      int
	NumPools      int
	Resources     []ResourceType
	RatePerSec    int
	OutofOrderPct float64 // 0..1
	LatePct       float64 // 0..1
	DuplicatePct  float64 // 0..1
	WindowSize    time.Duration
	Seed          int64
}

// Source generates simulated node resource samples and writes them to a submitter.
type Source struct {
	cfg       SourceConfig
	submitter Submitter
	rng       *rand.Rand
	seq       uint64
	history   []Sample
	nodePools []string
	baseTime  time.Time
}

// NewSource constructs a simulated source.
func NewSource(cfg SourceConfig, submitter Submitter) *Source {
	if cfg.Resources == nil {
		cfg.Resources = AllResources()
	}
	if cfg.NumNodes <= 0 {
		cfg.NumNodes = 10
	}
	if cfg.NumPools <= 0 {
		cfg.NumPools = 2
	}
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = time.Minute
	}
	nodePools := make([]string, cfg.NumNodes)
	for i := range nodePools {
		nodePools[i] = fmt.Sprintf("pool-%d", i%cfg.NumPools)
	}
	return &Source{
		cfg:       cfg,
		submitter: submitter,
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		history:   make([]Sample, 0, 16),
		nodePools: nodePools,
		baseTime:  time.Now().Truncate(cfg.WindowSize),
	}
}

// Run generates samples at the configured rate until ctx is canceled.
func (s *Source) Run(ctx context.Context) error {
	if s.cfg.RatePerSec <= 0 {
		s.cfg.RatePerSec = 100
	}
	interval := time.Second / time.Duration(s.cfg.RatePerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.next(ctx); err != nil {
				return err
			}
		}
	}
}

// RunN generates exactly n samples as fast as possible.
func (s *Source) RunN(ctx context.Context, n int) error {
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := s.next(ctx); err != nil {
			return err
		}
	}
	return nil
}

// next generates one sample (plus possible duplicate) and submits it.
func (s *Source) next(ctx context.Context) error {
	s.seq++
	sample := s.generate(s.seq)
	// Inject duplicate from history.
	if len(s.history) > 0 && s.rng.Float64() < s.cfg.DuplicatePct {
		dup := s.history[s.rng.Intn(len(s.history))]
		if err := s.submitter.SubmitContext(ctx, dup); err != nil {
			return err
		}
	}
	if err := s.submitter.SubmitContext(ctx, sample); err != nil {
		return err
	}
	if len(s.history) < 16 {
		s.history = append(s.history, sample)
	} else {
		s.history[s.rng.Intn(16)] = sample
	}
	return nil
}

func (s *Source) generate(seq uint64) Sample {
	nodeIdx := s.rng.Intn(s.cfg.NumNodes)
	resource := s.cfg.Resources[s.rng.Intn(len(s.cfg.Resources))]
	eventTime := s.baseTime.Add(time.Duration(seq) * time.Second)
	if s.rng.Float64() < s.cfg.OutofOrderPct {
		eventTime = eventTime.Add(-time.Duration(s.rng.Intn(5)+1) * time.Second)
	}
	if s.rng.Float64() < s.cfg.LatePct {
		eventTime = eventTime.Add(-s.cfg.WindowSize * 2)
	}
	value := s.rng.Float64() * 100
	return Sample{
		NodeID:    fmt.Sprintf("node-%d", nodeIdx),
		PoolID:    s.nodePools[nodeIdx],
		Resource:  resource,
		Value:     value,
		EventTime: eventTime,
		Seq:       seq,
	}
}
