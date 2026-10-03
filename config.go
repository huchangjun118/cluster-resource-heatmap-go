package heatmap

import (
	"errors"
	"time"
)

// Config holds all service configuration.
type Config struct {
	WindowSize      time.Duration
	WindowSlide     time.Duration
	AllowedLateness time.Duration
	MaxOpenWindows  int
	MaxSeen         int
	QueueCapacity   int
	DropPolicy      DropPolicy
	WALPath         string
	SnapshotPath    string
	SnapshotEvery   int
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() Config {
	return Config{
		WindowSize:      time.Minute,
		WindowSlide:     time.Minute,
		AllowedLateness: 10 * time.Second,
		MaxOpenWindows:  1000,
		MaxSeen:         100000,
		QueueCapacity:   1024,
		DropPolicy:      DropPolicyBlock,
		SnapshotEvery:   1000,
	}
}

// Validate checks the configuration for errors.
func (c Config) Validate() error {
	if c.WindowSize <= 0 {
		return errors.New("window size must be positive")
	}
	if c.WindowSlide <= 0 {
		return errors.New("window slide must be positive")
	}
	if c.WindowSlide > c.WindowSize {
		return errors.New("window slide must not exceed window size")
	}
	if c.WindowSize%c.WindowSlide != 0 {
		return errors.New("window size must be a multiple of slide")
	}
	if c.AllowedLateness < 0 {
		return errors.New("allowed lateness must be non-negative")
	}
	if c.QueueCapacity <= 0 {
		return errors.New("queue capacity must be positive")
	}
	return nil
}
