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
// and live SSE subscribers. Derived handlers from WithAttrs/WithGroup share the same bus & lock
// and preserve derived attributes/groups in Console & SSE output.
type LogBroadcaster struct {
	bus    *logBus
	inner  slog.Handler
	attrs  []slog.Attr
	groups []string
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
	target := entry
	for _, g := range l.groups {
		sub := make(map[string]any)
		target[g] = sub
		target = sub
	}
	for _, a := range l.attrs {
		addSlogAttr(target, a)
	}
	r.Attrs(func(a slog.Attr) bool {
		addSlogAttr(target, a)
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

func addSlogAttr(dst map[string]any, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Key == "" {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		sub := make(map[string]any)
		for _, ga := range a.Value.Group() {
			addSlogAttr(sub, ga)
		}
		dst[a.Key] = sub
		return
	}
	v := a.Value.Any()
	if err, ok := v.(error); ok && err != nil {
		dst[a.Key] = err.Error()
		return
	}
	dst[a.Key] = v
}

func (l *LogBroadcaster) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make([]slog.Attr, 0, len(l.attrs)+len(attrs))
	merged = append(merged, l.attrs...)
	merged = append(merged, attrs...)
	return &LogBroadcaster{
		bus:    l.bus,
		inner:  l.inner.WithAttrs(attrs),
		attrs:  merged,
		groups: append([]string(nil), l.groups...),
	}
}

func (l *LogBroadcaster) WithGroup(name string) slog.Handler {
	groups := append([]string(nil), l.groups...)
	if name != "" {
		groups = append(groups, name)
	}
	return &LogBroadcaster{
		bus:    l.bus,
		inner:  l.inner.WithGroup(name),
		attrs:  append([]slog.Attr(nil), l.attrs...),
		groups: groups,
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
