package nats

import (
	"context"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
)

type fakeCancellationSender struct {
	publishes int
	flushes   int
}

func (f *fakeCancellationSender) sendAndFlush() {
	f.publishes++
	f.flushes++
}

func (f *fakeCancellationSender) assertExactlyOne(t *testing.T) {
	t.Helper()
	if f.publishes != 1 || f.flushes != 1 {
		t.Fatalf("expected exactly one published and flushed cancellation, got publishes=%d flushes=%d", f.publishes, f.flushes)
	}
}

func TestCancellationBeforeReadySendsWhenReadinessArrives(t *testing.T) {
	sender := &fakeCancellationSender{}
	state := newRequestCancellationState(sender.sendAndFlush)
	if ready := state.requestCancel(); ready {
		t.Fatal("readiness unexpectedly observed before request_started")
	}

	messages := make(chan *gonats.Msg, 1)
	messages <- &gonats.Msg{Data: []byte(`{"type":"request_started"}`)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	awaitRequestStarted(ctx, messages, state)

	sender.assertExactlyOne(t)
}

func TestReadinessBeforeCancellationSendsImmediately(t *testing.T) {
	sender := &fakeCancellationSender{}
	state := newRequestCancellationState(sender.sendAndFlush)
	state.observeReady()
	if ready := state.requestCancel(); !ready {
		t.Fatal("expected readiness to remain observed")
	}

	sender.assertExactlyOne(t)
}

func TestRepeatedCancellationAndReadinessSignalsSendOnce(t *testing.T) {
	sender := &fakeCancellationSender{}
	state := newRequestCancellationState(sender.sendAndFlush)
	state.requestCancel()
	state.requestCancel()
	state.observeReady()
	state.observeReady()
	state.requestCancel()

	sender.assertExactlyOne(t)
}

func TestAwaitRequestStartedTimeoutReturnsWithoutSending(t *testing.T) {
	sender := &fakeCancellationSender{}
	state := newRequestCancellationState(sender.sendAndFlush)
	state.requestCancel()
	messages := make(chan *gonats.Msg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		awaitRequestStarted(ctx, messages, state)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("readiness waiter leaked after timeout")
	}
	if sender.publishes != 0 || sender.flushes != 0 {
		t.Fatalf("unexpected cancellation without readiness: %#v", sender)
	}
}
