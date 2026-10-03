package heatmap

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// 持久化布局（DataDir 下）：
//   wal.log      已接受且未清理的采样，JSON 行，崩溃后重放；
//   snapshot.json 已封口单元、修订记录、水位，原子重写（tmp+rename）。
//
// 恢复 = 加载 snapshot.json + 重放 wal.log：
//   - 未清理窗口（含补算期内的已封口窗口）从 WAL 重建原始采样，
//     已封口单元用全量采样重算，版本沿用持久化值；
//   - 已清理窗口的单元直接来自 snapshot.json；
//   - SampleID 去重使重放幂等，不重复计入也不丢失。

// snapshotFile 快照文件结构。
type snapshotFile struct {
	Watermark   int64      `json:"watermark"`
	MaxEvent    int64      `json:"max_event"`
	SealedCells []Cell     `json:"sealed_cells"`
	Revisions   []Revision `json:"revisions"`
}

// persistStore 持久化存储。dir 为空时为纯内存空实现（全部操作 no-op）。
type persistStore struct {
	dir string
	wal *os.File
	w   *bufio.Writer
}

func openPersistStore(dir string) (*persistStore, error) {
	p := &persistStore{dir: dir}
	if dir == "" {
		return p, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	p.wal = f
	p.w = bufio.NewWriter(f)
	return p, nil
}

// appendSample 追加一条已接受采样到 WAL 并落盘。
func (p *persistStore) appendSample(s Sample) error {
	if p.wal == nil {
		return nil
	}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if _, err := p.w.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := p.w.Flush(); err != nil {
		return err
	}
	return p.wal.Sync()
}

// saveSnapshot 原子重写快照文件。
func (p *persistStore) saveSnapshot(sf snapshotFile) error {
	if p.dir == "" {
		return nil
	}
	sort.Slice(sf.SealedCells, func(i, j int) bool {
		if !sf.SealedCells[i].Window.Start.Equal(sf.SealedCells[j].Window.Start) {
			return sf.SealedCells[i].Window.Start.Before(sf.SealedCells[j].Window.Start)
		}
		if sf.SealedCells[i].NodeID != sf.SealedCells[j].NodeID {
			return sf.SealedCells[i].NodeID < sf.SealedCells[j].NodeID
		}
		return sf.SealedCells[i].Resource < sf.SealedCells[j].Resource
	})
	b, err := json.Marshal(sf)
	if err != nil {
		return err
	}
	tmp := filepath.Join(p.dir, "snapshot.json.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(p.dir, "snapshot.json"))
}

// loadSnapshot 加载快照文件；不存在时返回零值。
func (p *persistStore) loadSnapshot() (snapshotFile, error) {
	var sf snapshotFile
	if p.dir == "" {
		return sf, nil
	}
	b, err := os.ReadFile(filepath.Join(p.dir, "snapshot.json"))
	if errors.Is(err, os.ErrNotExist) {
		return sf, nil
	}
	if err != nil {
		return sf, err
	}
	return sf, json.Unmarshal(b, &sf)
}

// loadWAL 读取 WAL 中全部采样。
func (p *persistStore) loadWAL() ([]Sample, error) {
	if p.dir == "" {
		return nil, nil
	}
	f, err := os.Open(filepath.Join(p.dir, "wal.log"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Sample
	dec := json.NewDecoder(f)
	for {
		var s Sample
		if err := dec.Decode(&s); err != nil {
			if err == io.EOF {
				break
			}
			return out, fmt.Errorf("wal decode: %w", err)
		}
		out = append(out, s)
	}
	return out, nil
}

// checkpoint 压缩 WAL：仅保留未清理窗口的采样，原子重写。
func (p *persistStore) checkpoint(keep []Sample) error {
	if p.dir == "" {
		return nil
	}
	if err := p.w.Flush(); err != nil {
		return err
	}
	sort.Slice(keep, func(i, j int) bool { return keep[i].SampleID < keep[j].SampleID })
	tmp := filepath.Join(p.dir, "wal.log.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, s := range keep {
		b, err := json.Marshal(s)
		if err != nil {
			f.Close()
			return err
		}
		if _, err := w.Write(append(b, '\n')); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// 先关闭当前 WAL 句柄，再原子替换，最后重新打开。
	if p.wal != nil {
		p.wal.Close()
	}
	if err := os.Rename(tmp, filepath.Join(p.dir, "wal.log")); err != nil {
		return err
	}
	nf, err := os.OpenFile(filepath.Join(p.dir, "wal.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	p.wal = nf
	p.w = bufio.NewWriter(nf)
	return nil
}

// close 关闭存储。
func (p *persistStore) close() error {
	if p.wal == nil {
		return nil
	}
	if err := p.w.Flush(); err != nil {
		return err
	}
	return p.wal.Close()
}

// timeFromNano 辅助：unix nano -> time。
func timeFromNano(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}
