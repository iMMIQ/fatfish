package parser

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParserSingleflightCoalescesConcurrentFetches(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		time.Sleep(40 * time.Millisecond)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Concurrent Test Article</title><meta name="description" content="Singleflight coalescing works cleanly."></head><body><p>Hello world content.</p></body></html>`))
	}))
	defer srv.Close()

	svc := NewService(Options{
		AllowHosts: []string{"127.0.0.1"},
		Timeout:    5 * time.Second,
		CacheTTL:   5 * time.Minute,
	})

	const callers = 8
	var wg sync.WaitGroup
	errs := make([]error, callers)
	titles := make([]string, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			res, err := svc.Parse(context.Background(), srv.URL+"/post/1", ParseOptions{})
			errs[idx] = err
			if res != nil {
				titles[idx] = res.Title
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d failed: %v", i, errs[i])
		}
		if titles[i] != "Concurrent Test Article" {
			t.Fatalf("caller %d unexpected title %q", i, titles[i])
		}
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected singleflight to coalesce %d concurrent requests into 1 upstream fetch, got %d", callers, got)
	}
}
