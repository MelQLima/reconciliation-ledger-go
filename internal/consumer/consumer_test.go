package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/MelQLima/conciliacao-ledger-go/internal/consumer"
	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
)

// fakeSource returns queued messages, then blocks until ctx is cancelled.
type fakeSource struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	committed []int64
	done      chan struct{}
}

func (f *fakeSource) FetchMessage(ctx context.Context) (kafka.Message, error) {
	f.mu.Lock()
	if len(f.msgs) > 0 {
		m := f.msgs[0]
		f.msgs = f.msgs[1:]
		f.mu.Unlock()
		return m, nil
	}
	f.mu.Unlock()
	close(f.done)
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *fakeSource) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range msgs {
		f.committed = append(f.committed, m.Offset)
	}
	return nil
}

type fakeSink struct{ msgs []kafka.Message }

func (f *fakeSink) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.msgs = append(f.msgs, msgs...)
	return nil
}

// flakyApplier fails the first `failures` calls with a transient error.
type flakyApplier struct {
	failures int
	calls    int
	err      error
}

func (a *flakyApplier) Apply(_ context.Context, e ledger.PaymentEvent) (ledger.Result, error) {
	a.calls++
	if a.err != nil {
		return "", a.err
	}
	if a.calls <= a.failures {
		return "", errors.New("connection reset by peer")
	}
	if _, err := ledger.FromEvent(e); err != nil {
		return ledger.ResultInvalid, err
	}
	return ledger.ResultRecorded, nil
}

func msg(t *testing.T, offset int64, e ledger.PaymentEvent) kafka.Message {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "payments.events", Offset: offset, Key: []byte(e.EndToEndID), Value: b}
}

func validEvent() ledger.PaymentEvent {
	return ledger.PaymentEvent{EventID: "evt-1", Type: ledger.EventPixSettled, EndToEndID: "E2E1",
		PayerAccount: "c1", PayeeAccount: "m1", AmountCents: 100, OccurredAt: time.Now()}
}

func run(t *testing.T, src *fakeSource, sink *fakeSink, app consumer.Applier) []string {
	t.Helper()
	var mu sync.Mutex
	var results []string
	c := consumer.New(src, sink, app, slog.New(slog.NewTextHandler(io.Discard, nil)), consumer.Options{
		MaxRetries: 3,
		BaseDelay:  time.Millisecond,
		OnResult:   func(r string) { mu.Lock(); results = append(results, r); mu.Unlock() },
	})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	<-src.done
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("Run: %v", err)
	}
	return results
}

func TestRetriesTransientErrorsThenCommits(t *testing.T) {
	src := &fakeSource{msgs: []kafka.Message{msg(t, 7, validEvent())}, done: make(chan struct{})}
	sink := &fakeSink{}
	app := &flakyApplier{failures: 2}

	results := run(t, src, sink, app)

	if app.calls != 3 {
		t.Fatalf("calls = %d, want 3", app.calls)
	}
	if len(sink.msgs) != 0 {
		t.Fatalf("unexpected dead letters: %d", len(sink.msgs))
	}
	if len(src.committed) != 1 || src.committed[0] != 7 {
		t.Fatalf("committed = %v", src.committed)
	}
	if len(results) != 1 || results[0] != "recorded" {
		t.Fatalf("results = %v", results)
	}
}

func TestPoisonMessagesGoToDeadLetter(t *testing.T) {
	invalid := validEvent()
	invalid.AmountCents = 0
	src := &fakeSource{msgs: []kafka.Message{
		{Topic: "payments.events", Offset: 1, Value: []byte("{not json")},
		msg(t, 2, invalid),
	}, done: make(chan struct{})}
	sink := &fakeSink{}

	results := run(t, src, sink, &flakyApplier{})

	if len(sink.msgs) != 2 {
		t.Fatalf("dead letters = %d, want 2", len(sink.msgs))
	}
	for _, m := range sink.msgs {
		if !hasHeader(m, "x-error") || !hasHeader(m, "x-original-topic") {
			t.Fatalf("missing headers: %+v", m.Headers)
		}
	}
	if len(src.committed) != 2 {
		t.Fatalf("poison messages must be committed so the partition is not blocked, got %v", src.committed)
	}
	if len(results) != 2 || results[0] != "dead_letter" || results[1] != "dead_letter" {
		t.Fatalf("results = %v", results)
	}
}

func TestGivesUpAfterMaxRetries(t *testing.T) {
	src := &fakeSource{msgs: []kafka.Message{msg(t, 1, validEvent())}, done: make(chan struct{})}
	sink := &fakeSink{}
	app := &flakyApplier{err: errors.New("database is down")}

	run(t, src, sink, app)

	if app.calls != 4 { // first try + 3 retries
		t.Fatalf("calls = %d, want 4", app.calls)
	}
	if len(sink.msgs) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(sink.msgs))
	}
}

func hasHeader(m kafka.Message, key string) bool {
	for _, h := range m.Headers {
		if h.Key == key {
			return true
		}
	}
	return false
}
