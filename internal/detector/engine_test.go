package detector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"sentinel/internal/alert"
	"sentinel/internal/event"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type recordCall struct {
	rule, eventID string
}

type fakeStore struct {
	mu    sync.Mutex
	calls []recordCall
	err   error
}

func (s *fakeStore) Record(_ context.Context, f alert.Finding, eventID string, _ time.Time) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, false, s.err
	}
	s.calls = append(s.calls, recordCall{f.Rule, eventID})
	return 1, len(s.calls) == 1, nil
}

type alwaysFires struct{}

func (alwaysFires) Name() string { return "always" }
func (alwaysFires) Evaluate(_ context.Context, ev event.Event) (*alert.Finding, error) {
	return &alert.Finding{Rule: "always", Severity: alert.SeverityLow, Entity: ev.User}, nil
}

func TestEngineSkipsAlreadyProcessedEvents(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{}
	e := NewEngine([]Rule{alwaysFires{}}, store, newRedis(t), time.Hour, discard)
	ev := login("alice", "198.51.100.1", "DE", event.OutcomeFailure, base)

	dup, err := e.Handle(ctx, ev)
	if err != nil || dup {
		t.Fatalf("first delivery: dup=%v err=%v", dup, err)
	}
	dup, err = e.Handle(ctx, ev)
	if err != nil || !dup {
		t.Fatalf("second delivery should be a duplicate: dup=%v err=%v", dup, err)
	}
	if len(store.calls) != 1 {
		t.Fatalf("finding recorded %d times, want 1", len(store.calls))
	}
}

func TestEngineDoesNotMarkProcessedOnStoreError(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{err: errors.New("postgres down")}
	e := NewEngine([]Rule{alwaysFires{}}, store, newRedis(t), time.Hour, discard)
	ev := login("alice", "198.51.100.1", "DE", event.OutcomeFailure, base)

	if _, err := e.Handle(ctx, ev); err == nil {
		t.Fatal("expected error")
	}

	store.err = nil
	dup, err := e.Handle(ctx, ev)
	if err != nil || dup {
		t.Fatalf("retry after failure: dup=%v err=%v", dup, err)
	}
	if len(store.calls) != 1 {
		t.Fatalf("want 1 recorded finding, got %d", len(store.calls))
	}
}

type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	committed []int64
	cancel    context.CancelFunc
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.msgs) == 0 {
		r.cancel()
		return kafka.Message{}, ctx.Err()
	}
	m := r.msgs[0]
	r.msgs = r.msgs[1:]
	return m, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

type fakeWriter struct{ msgs []kafka.Message }

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.msgs = append(w.msgs, msgs...)
	return nil
}

type flakyHandler struct {
	failures int
	handled  []string
}

func (h *flakyHandler) Handle(_ context.Context, ev event.Event) (bool, error) {
	if h.failures > 0 {
		h.failures--
		return false, errors.New("redis timeout")
	}
	h.handled = append(h.handled, ev.ID)
	return false, nil
}

func TestConsumerDeadLettersPoisonAndRetriesTransient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	good := `{"event_id":"6f1c2c1e-6b1d-4a52-9d55-2f5c0f6b7a10","type":"auth.login","outcome":"failure",
		"user":"alice","src_ip":"203.0.113.1","country":"NL","timestamp":"2026-11-12T10:00:00Z"}`
	reader := &fakeReader{cancel: cancel, msgs: []kafka.Message{
		{Offset: 1, Value: []byte("garbage")},
		{Offset: 2, Value: []byte(good)},
	}}
	dlq := &fakeWriter{}
	h := &flakyHandler{failures: 2}

	c := NewConsumer(reader, dlq, h, discard)
	c.backoff = Backoff{Base: time.Millisecond, Max: time.Millisecond}
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if len(dlq.msgs) != 1 || string(dlq.msgs[0].Value) != "garbage" {
		t.Fatalf("poison message should be dead-lettered once, got %d", len(dlq.msgs))
	}
	if len(h.handled) != 1 {
		t.Fatalf("good message should be handled after retries, handled=%v", h.handled)
	}
	if len(reader.committed) != 2 || reader.committed[0] != 1 || reader.committed[1] != 2 {
		t.Fatalf("both offsets must be committed in order, got %v", reader.committed)
	}
}

func TestBackoff(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: time.Second}
	want := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := b.Delay(i); got != w*time.Millisecond {
			t.Fatalf("Delay(%d) = %v, want %v", i, got, w*time.Millisecond)
		}
	}
	if b.Delay(1000) != time.Second {
		t.Fatal("huge attempt numbers must not overflow")
	}
}
