package infer

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned immediately when a capability circuit breaker is open.
var ErrCircuitOpen = errors.New("infer: circuit breaker is open")

// BreakerState represents the state of a per-capability circuit breaker.
type BreakerState string

const (
	StateClosed   BreakerState = "closed"
	StateOpen     BreakerState = "open"
	StateHalfOpen BreakerState = "half_open"
)

// BreakerSnapshot is exported to the admin console dashboard.
type BreakerSnapshot struct {
	Name        string       `json:"name"`
	State       BreakerState `json:"state"`
	Failures    int          `json:"failures"`
	LastError   string       `json:"last_error,omitempty"`
	OpenUntil   time.Time    `json:"open_until,omitempty"`
	TotalCalls  int64        `json:"total_calls"`
	TotalErrors int64        `json:"total_errors"`
}

// CircuitBreaker implements 3 consecutive failures -> Open (30s) -> Half-Open single probe.
type CircuitBreaker struct {
	mu               sync.Mutex
	name             string
	failureThreshold int
	openDuration     time.Duration

	state         BreakerState
	failures      int
	openUntil     time.Time
	probeInFlight bool
	lastError     string
	totalCalls    int64
	totalErrors   int64
}

func NewCircuitBreaker(name string, failureThreshold int, openDuration time.Duration) *CircuitBreaker {
	if failureThreshold <= 0 {
		failureThreshold = 3
	}
	if openDuration <= 0 {
		openDuration = 30 * time.Second
	}
	return &CircuitBreaker{
		name:             name,
		failureThreshold: failureThreshold,
		openDuration:     openDuration,
		state:            StateClosed,
	}
}

// Allow checks if a call may proceed.
func (cb *CircuitBreaker) Allow() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	switch cb.state {
	case StateClosed:
		cb.totalCalls++
		return nil
	case StateOpen:
		if now.Before(cb.openUntil) {
			return ErrCircuitOpen
		}
		cb.state = StateHalfOpen
		cb.probeInFlight = true
		cb.totalCalls++
		return nil
	case StateHalfOpen:
		if cb.probeInFlight {
			return ErrCircuitOpen
		}
		cb.probeInFlight = true
		cb.totalCalls++
		return nil
	}
	return nil
}

// Record records the outcome of a call that passed Allow().
func (cb *CircuitBreaker) Record(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err == nil {
		cb.state = StateClosed
		cb.failures = 0
		cb.probeInFlight = false
		cb.lastError = ""
		return
	}

	cb.totalErrors++
	cb.lastError = err.Error()
	cb.failures++
	cb.probeInFlight = false
	if cb.state == StateHalfOpen || cb.failures >= cb.failureThreshold {
		cb.state = StateOpen
		cb.openUntil = time.Now().Add(cb.openDuration)
	}
}

// Snapshot returns the current diagnostic snapshot.
func (cb *CircuitBreaker) Snapshot() BreakerSnapshot {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	st := cb.state
	if st == StateOpen && time.Now().After(cb.openUntil) {
		st = StateHalfOpen
	}
	return BreakerSnapshot{
		Name:        cb.name,
		State:       st,
		Failures:    cb.failures,
		LastError:   cb.lastError,
		OpenUntil:   cb.openUntil,
		TotalCalls:  cb.totalCalls,
		TotalErrors: cb.totalErrors,
	}
}
