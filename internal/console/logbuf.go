package console

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"time"
)

// LogBroadcaster is an slog.Handler wrapper that tees JSON logs to a bounded ring buffer
// and live SSE subscribers.
type LogBroadcaster struct {
	mu      sync.RWMutex
	ring    []string
	maxSize int
	subs    map[chan string]struct{}
	inner   slog.Handler
}

func NewLogBroadcaster(out io.Writer, level slog.Level, maxSize int) *LogBroadcaster {
	if maxSize <= 0 {
		maxSize = 200
	}
	return &LogBroadcaster{
		maxSize: maxSize,
		subs:    make(map[chan string]struct{}),
		inner:   slog.NewJSONHandler(out, &slog.HandlerOptions{Level: level}),
	}
}

func (l *LogBroadcaster) Enabled(ctx context.Context, lvl slog.Level) bool {
	return l.inner.Enabled(ctx, lvl)
}

func (l *LogBroadcaster) Handle(ctx context.Context, r slog.Record) error {
	_ = l.inner.Handle(ctx, r)
	entry := map[string]any{
		"time":  r.Time.Format(time.RFC3339),
		"level": r.Level.String(),
		"msg":   r.Message,
	}
	r.Attrs(func(a slog.Attr) bool {
		entry[a.Key] = a.Value.Any()
		return true
	})
	raw, _ := json.Marshal(entry)
	line := string(raw)

	l.mu.Lock()
	l.ring = append(l.ring, line)
	if len(l.ring) > l.maxSize {
		l.ring = l.ring[len(l.ring)-l.maxSize:]
	}
	for ch := range l.subs {
		select {
		case ch <- line:
		default:
		}
	}
	l.mu.Unlock()
	return nil
}

func (l *LogBroadcaster) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LogBroadcaster{
		ring:    l.ring,
		maxSize: l.maxSize,
		subs:    l.subs,
		inner:   l.inner.WithAttrs(attrs),
	}
}

func (l *LogBroadcaster) WithGroup(name string) slog.Handler {
	return &LogBroadcaster{
		ring:    l.ring,
		maxSize: l.maxSize,
		subs:    l.subs,
		inner:   l.inner.WithGroup(name),
	}
}

func (l *LogBroadcaster) Recent() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]string(nil), l.ring...)
}

func (l *LogBroadcaster) Subscribe() (chan string, func()) {
	ch := make(chan string, 64)
	l.mu.Lock()
	l.subs[ch] = struct{}{}
	l.mu.Unlock()
	unsub := func() {
		l.mu.Lock()
		delete(l.subs, ch)
		l.mu.Unlock()
	}
	return ch, unsub
}
