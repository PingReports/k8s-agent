package buffer

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// FileBuffer persists unsent batches to disk so a transient ingest outage
// doesn't lose data. Each batch is a gzipped JSON file named by its
// timestamp; old files past the retention window are pruned.
type FileBuffer struct {
	dir       string
	retention time.Duration
	mu        sync.Mutex
}

func New(dir string, retention time.Duration) (*FileBuffer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileBuffer{dir: dir, retention: retention}, nil
}

func (b *FileBuffer) Enqueue(payload any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("%d.json.gz", time.Now().UnixNano())
	tmp := filepath.Join(b.dir, name+".tmp")
	final := filepath.Join(b.dir, name)
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	gz, _ := gzip.NewWriterLevel(f, gzip.BestCompression)
	if _, err := gz.Write(raw); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := gz.Close(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, final)
}

func (b *FileBuffer) ListOldest(n int) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ents, err := os.ReadDir(b.dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		nm := e.Name()
		if e.IsDir() || !strings.HasSuffix(nm, ".json.gz") {
			continue
		}
		names = append(names, nm)
	}
	sort.Strings(names)
	if n > 0 && len(names) > n {
		names = names[:n]
	}
	out := make([]string, len(names))
	for i, nm := range names {
		out[i] = filepath.Join(b.dir, nm)
	}
	return out, nil
}

func (b *FileBuffer) ReadAndRemove(path string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	raw, err := io.ReadAll(gz)
	if err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil {
		return raw, err
	}
	return raw, nil
}

// Prune removes batches older than the retention window. Called periodically
// from the main loop.
func (b *FileBuffer) Prune() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	ents, err := os.ReadDir(b.dir)
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-b.retention)
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(b.dir, e.Name()))
		}
	}
	return nil
}
