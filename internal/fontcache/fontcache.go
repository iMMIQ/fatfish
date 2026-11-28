package fontcache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/image/font/opentype"
)

var (
	mu    sync.RWMutex
	fonts = make(map[string]*opentype.Font)
)

// Load reads and parses a TrueType/OpenType font once per canonical file path,
// sharing the parsed *opentype.Font across ChartRenderer and travel visual renderers
// to avoid duplicate ~10-18 MiB CJK font allocations in RAM.
func Load(path string) (*opentype.Font, error) {
	key := canonicalKey(path)

	mu.RLock()
	f, ok := fonts[key]
	mu.RUnlock()
	if ok {
		return f, nil
	}

	mu.Lock()
	defer mu.Unlock()
	if f, ok := fonts[key]; ok {
		return f, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("fontcache: read font %s: %w", path, err)
	}
	parsed, err := opentype.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("fontcache: parse font %s: %w", path, err)
	}
	fonts[key] = parsed
	return parsed, nil
}

func canonicalKey(path string) string {
	clean := filepath.Clean(path)
	if abs, err := filepath.Abs(clean); err == nil {
		return abs
	}
	return clean
}
