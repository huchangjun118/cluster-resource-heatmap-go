package main

import (
	"context"
	"fmt"
	"os"
	"time"

	heatmap "github.com/huchangjun118/cluster-resource-heatmap-go"
)

func main() {
	cfg := heatmap.DefaultConfig()
	cfg.WALPath = "/tmp/heatmap.wal"
	cfg.SnapshotPath = "/tmp/heatmap.snap"

	svc, err := heatmap.NewService(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := svc.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer svc.Stop()

	srcCfg := heatmap.SourceConfig{
		NumNodes:   10,
		NumPools:   2,
		Resources:  heatmap.AllResources(),
		RatePerSec: 100,
		OutofOrderPct: 0.1,
		LatePct:    0.05,
		DuplicatePct: 0.02,
		WindowSize: cfg.WindowSize,
		Seed:       42,
	}
	src := heatmap.NewSource(srcCfg, svc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := src.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	if err := svc.Flush(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	view := svc.View()
	fmt.Printf("windows: %d\n", len(view.WindowStarts()))
	for _, ws := range view.WindowStarts() {
		cells := view.Heatmap(time.Unix(0, ws), time.Unix(0, ws).Add(cfg.WindowSize))
		fmt.Printf("window %s: %d cells\n", time.Unix(0, ws).Format(time.RFC3339), len(cells))
	}
	fmt.Printf("metrics: %+v\n", svc.Metrics())
}
