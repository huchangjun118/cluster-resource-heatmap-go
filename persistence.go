package heatmap

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"os"
	"sync"
	"time"
)

// WAL is an append-only write-ahead log of ingested samples.
type WAL struct {
	file *os.File
	path string
	mu   sync.Mutex
}

// StateSnapshot is a full point-in-time snapshot of sealed state.
type StateSnapshot struct {
	Sealed      map[int64]map[CellKey]Cell
	Revisions   []Revision
	Watermark   time.Time
	Seen        map[SampleKey]time.Time
	WALOffset   int64
	CommittedAt time.Time
}

func init() {
	gob.Register(ResourceType(""))
}

// NewWAL opens (or creates) a WAL file at the given path.
func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, path: path}, nil
}

// Append writes a sample to the WAL.
func (w *WAL) Append(s Sample) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if _, err := w.file.Write(append(data, '\n')); err != nil {
		return err
	}
	return w.file.Sync()
}

// Replay reads all samples from the WAL starting at the given byte offset.
func (w *WAL) Replay(offset int64, fn func(Sample) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.file.Seek(offset, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		var s Sample
		if err := json.Unmarshal(scanner.Bytes(), &s); err != nil {
			return err
		}
		if err := fn(s); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// Offset returns the current write position in the WAL.
func (w *WAL) Offset() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Seek(0, 1)
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// SaveSnapshot writes a full snapshot to the given path.
func SaveSnapshot(path string, snap StateSnapshot) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	return enc.Encode(snap)
}

// LoadSnapshot reads a snapshot from the given path.
func LoadSnapshot(path string) (*StateSnapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var snap StateSnapshot
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&snap); err != nil {
		return nil, err
	}
	return &snap, nil
}
