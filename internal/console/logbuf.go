package console

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"time"
)

type logBus struct {
	mu      sync.RWMutex
	ring    []string
	maxSize int
	subs    map[chan string]struct{}
}

// LogBroadcaster is an slog.Handler wrapper that tees JSON logs to a bounded ring buffer
// and live SSE subscribers. Derived handlers from WithAttrs/WithGroup share the same bus & lock.
type LogBroadcaster struct {
	bus   *logBus
	inner slog.Handler
}

func NewLogBroadcaster(out io.Writer, level slog.Level, maxSize int) *LogBroadcaster {
	if maxSize <= 0 {
		maxSize = 200
	}
	return &LogBroadcaster{
		bus: &logBus{
			maxSize: maxSize,
			subs:    make(map[chan string]struct{}),
		},
		inner: slog.NewJSONHandler(out, &slog.HandlerOptions{Level: level}),
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

	l.bus.mu.Lock()
	l.bus.ring = append(l.bus.ring, line)
	if len(l.bus.ring) > l.bus.maxSize {
		l.bus.ring = l.bus.ring[len(l.bus.ring)-l.bus.maxSize:]
	}
	for ch := range l.bus.subs {
		select {
		case ch <- line:
		default:
		}
	}
	l.bus.mu.Unlock()
	return nil
}

func (l *LogBroadcaster) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LogBroadcaster{
		bus:   l.bus,
		inner: l.inner.WithAttrs(attrs),
	}
}

func (l *LogBroadcaster) WithGroup(name string) slog.Handler {
	return &LogBroadcaster{
		bus:   l.bus,
		inner: l.inner.WithGroup(name),
	}
}

func (l *LogBroadcaster) Recent() []string {
	l.bus.mu.RLock()
	defer l.bus.mu.RUnlock()
	return append([]string(nil), l.bus.ring...)
}

func (l *LogBroadcaster) Subscribe() (chan string, func()) {
	ch := make(chan string, 64)
	l.bus.mu.Lock()
	l.bus.subs[ch] = struct{}{}
	l.bus.mu.Unlock()
	unsub := func() {
		l.bus.mu.Lock()
		delete(l.bus.subs, ch)
		l.bus.mu.Unlock()
	}
	return ch, unsub
}
