package heatmap

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrBackpressure 摄入队列已满，采样被拒绝（可观测的确定行为）。
var ErrBackpressure = errors.New("heatmap: ingest queue full, sample rejected")

// ErrClosed 服务已关闭。
var ErrClosed = errors.New("heatmap: service closed")

// Metrics 运行指标。并发安全，仅通过 atomic 访问。
type Metrics struct {
	Ingested      int64 // 接受的采样数
	Duplicates    int64 // 重复被拒绝数
	TooLate       int64 // 超出补算期被拒绝数
	Revisions     int64 // 修订次数
	Rejected      int64 // 背压拒绝数
	PersistErrors int64 // 持久化失败数
}

// Service 热力图数据服务：摄入管道 + 聚合引擎 + 快照发布。
//
// 并发模型：单一 run goroutine 串行处理队列并推进聚合，
// 每次处理后发布一个不可变快照；查询只读快照，
// 因此查询永远看到某个已提交的完整视图，慢查询不阻塞聚合。
type Service struct {
	cfg   Config
	queue chan Sample
	flush chan chan struct{} // 外部触发检查点的控制通道
	agg   *aggregator
	store *persistStore

	snap atomic.Pointer[Snapshot]

	metrics Metrics

	mu     sync.RWMutex // 保护 closed 与 queue 关闭
	closed bool
	wg     sync.WaitGroup
}

// NewService 创建并启动服务。DataDir 非空时从磁盘恢复状态。
func NewService(cfg Config) (*Service, error) {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = time.Minute
	}
	if cfg.SlideStep <= 0 {
		cfg.SlideStep = cfg.WindowSize
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 1024
	}
	if cfg.SnapshotEvery <= 0 {
		cfg.SnapshotEvery = time.Second
	}
	store, err := openPersistStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	agg := newAggregator(cfg)
	sf, err := store.loadSnapshot()
	if err != nil {
		store.close()
		return nil, err
	}
	walSamples, err := store.loadWAL()
	if err != nil {
		store.close()
		return nil, err
	}
	agg.recover(sf, walSamples)

	s := &Service{
		cfg:   cfg,
		queue: make(chan Sample, cfg.QueueCapacity),
		flush: make(chan chan struct{}),
		agg:   agg,
		store: store,
	}
	s.snap.Store(agg.snapshot())
	s.wg.Add(1)
	go s.run()
	return s, nil
}

// Ingest 提交一条采样。队列满时立即返回 ErrBackpressure（确定行为）；
// ctx 已取消时不入队，不会产生部分写入。
func (s *Service) Ingest(ctx context.Context, sample Sample) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	select {
	case s.queue <- sample:
		return nil
	default:
		atomic.AddInt64(&s.metrics.Rejected, 1)
		return ErrBackpressure
	}
}

// Checkpoint 强制立即执行一次检查点（快照落盘 + WAL 压缩），
// 完成后返回。供测试与运维使用；通过控制通道转发到 run goroutine，
// 不破坏单写者约束。
func (s *Service) Checkpoint() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}
	done := make(chan struct{})
	s.flush <- done
	<-done
}

// Snapshot 获取当前已提交视图。
func (s *Service) Snapshot() *Snapshot {
	return s.snap.Load()
}

// Query 在当前已提交视图上执行查询。
func (s *Service) Query(q Query, groupBy GroupBy) []Row {
	return QuerySnapshot(s.Snapshot(), q, groupBy)
}

// Metrics 返回运行指标快照。
func (s *Service) Metrics() Metrics {
	return Metrics{
		Ingested:      atomic.LoadInt64(&s.metrics.Ingested),
		Duplicates:    atomic.LoadInt64(&s.metrics.Duplicates),
		TooLate:       atomic.LoadInt64(&s.metrics.TooLate),
		Revisions:     atomic.LoadInt64(&s.metrics.Revisions),
		Rejected:      atomic.LoadInt64(&s.metrics.Rejected),
		PersistErrors: atomic.LoadInt64(&s.metrics.PersistErrors),
	}
}

// Close 停止服务：拒绝新摄入、排空队列、落盘检查点、关闭存储。
func (s *Service) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.queue)
	s.mu.Unlock()

	s.wg.Wait() // run goroutine 排空队列并完成最终检查点
	return s.store.close()
}

// run 摄入主循环：串行消费队列，保证状态单写者。
func (s *Service) run() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.SnapshotEvery)
	defer ticker.Stop()
	for {
		select {
		case sample, ok := <-s.queue:
			if !ok {
				s.checkpoint() // 最终落盘
				s.snap.Store(s.agg.snapshot())
				return
			}
			s.process(sample)
		case <-ticker.C:
			s.checkpoint()
		case done := <-s.flush:
			s.checkpoint()
			close(done)
		}
	}
}

// process 处理单条采样：聚合、持久化、发布快照。
func (s *Service) process(sample Sample) {
	res := s.agg.ingest(sample)
	switch {
	case res.duplicate:
		atomic.AddInt64(&s.metrics.Duplicates, 1)
	case res.tooLate:
		atomic.AddInt64(&s.metrics.TooLate, 1)
	case res.accepted:
		atomic.AddInt64(&s.metrics.Ingested, 1)
		if err := s.store.appendSample(sample); err != nil {
			atomic.AddInt64(&s.metrics.PersistErrors, 1)
		}
		if len(res.sealed) > 0 || len(res.revised) > 0 {
			if n := len(res.revisions); n > 0 {
				atomic.AddInt64(&s.metrics.Revisions, int64(n))
			}
			if err := s.store.saveSnapshot(s.agg.sealedSnapshot()); err != nil {
				atomic.AddInt64(&s.metrics.PersistErrors, 1)
			}
		}
	}
	s.snap.Store(s.agg.snapshot())
}

// checkpoint 压缩 WAL 并保存快照。
func (s *Service) checkpoint() {
	if err := s.store.saveSnapshot(s.agg.sealedSnapshot()); err != nil {
		atomic.AddInt64(&s.metrics.PersistErrors, 1)
	}
	if err := s.store.checkpoint(s.agg.checkpointSamples()); err != nil {
		atomic.AddInt64(&s.metrics.PersistErrors, 1)
	}
}
