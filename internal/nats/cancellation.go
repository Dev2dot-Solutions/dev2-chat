package nats

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	gonats "github.com/nats-io/nats.go"
)

// requestCancellationState retains cancellation intent until the runtime has
// acknowledged request readiness. send must publish and flush synchronously.
type requestCancellationState struct {
	mu        sync.Mutex
	ready     bool
	requested bool
	sent      bool
	send      func()
}

func newRequestCancellationState(send func()) *requestCancellationState {
	return &requestCancellationState{send: send}
}

func (s *requestCancellationState) observeReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = true
	s.sendIfReady()
}

// requestCancel records cancellation intent and reports whether readiness was
// already observed. If ready, the cancellation has been sent and flushed
// before this method returns.
func (s *requestCancellationState) requestCancel() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requested = true
	s.sendIfReady()
	return s.ready
}

func (s *requestCancellationState) sendIfReady() {
	if !s.ready || !s.requested || s.sent {
		return
	}
	s.sent = true
	if s.send != nil {
		s.send()
	}
}

// awaitRequestStarted keeps an early-cancelled request's progress inbox alive
// until readiness arrives or its independent bounded context expires.
func awaitRequestStarted(ctx context.Context, messages <-chan *gonats.Msg, state *requestCancellationState) {
	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				return
			}
			if msg == nil {
				continue
			}
			var event struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(msg.Data, &event) == nil && isRequestStarted(event.Type) {
				state.observeReady()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func isRequestStarted(eventType string) bool {
	normalized := strings.ReplaceAll(strings.ToLower(eventType), "-", "_")
	return normalized == "request_started"
}
