package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"sentinel/internal/event"
	"sentinel/internal/httpx"
)

type fakePublisher struct {
	got []event.Event
	err error
}

func (p *fakePublisher) Publish(_ context.Context, evs []event.Event) error {
	if p.err != nil {
		return p.err
	}
	p.got = append(p.got, evs...)
	return nil
}

type fakeLimiter struct {
	allow bool
	err   error
}

func (l fakeLimiter) Allow(context.Context, string, int) (bool, time.Duration, error) {
	return l.allow, 2 * time.Second, l.err
}

var now = time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)

const goodEvent = `{"event_id":"6f1c2c1e-6b1d-4a52-9d55-2f5c0f6b7a10","type":"auth.login","outcome":"failure",
	"user":"alice","src_ip":"203.0.113.1","country":"NL","timestamp":"2026-11-12T11:59:00Z"}`

func do(t *testing.T, h *Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	h.now = func() time.Time { return now }
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	req = req.WithContext(httpx.WithPrincipal(req.Context(), "test-agent"))
	rec := httptest.NewRecorder()
	h.Ingest(rec, req)
	return rec
}

func newHandler(pub Publisher, lim Limiter) *Handler {
	return NewHandler(pub, lim, slog.New(slog.NewTextHandler(io.Discard, nil)), 10)
}

func TestIngestAccepts(t *testing.T) {
	pub := &fakePublisher{}
	rec := do(t, newHandler(pub, fakeLimiter{allow: true}), `{"events":[`+goodEvent+`]}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
	if len(pub.got) != 1 || pub.got[0].User != "alice" {
		t.Fatalf("published %+v", pub.got)
	}
}

func TestIngestRejectsWholeBatchOnInvalidEvent(t *testing.T) {
	pub := &fakePublisher{}
	bad := strings.Replace(goodEvent, `"NL"`, `"nl"`, 1)
	rec := do(t, newHandler(pub, fakeLimiter{allow: true}), `{"events":[`+goodEvent+`,`+bad+`]}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status %d", rec.Code)
	}
	if len(pub.got) != 0 {
		t.Fatal("nothing may be published when any event is invalid")
	}
	var body struct {
		Error struct {
			Details []fieldError `json:"details"`
		} `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if len(body.Error.Details) != 1 || body.Error.Details[0].Index != 1 {
		t.Fatalf("expected error for index 1, got %+v", body.Error.Details)
	}
}

func TestIngestRequestErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{"malformed json", `{"events":`, http.StatusBadRequest},
		{"unknown field", `{"events":[],"x":1}`, http.StatusBadRequest},
		{"empty batch", `{"events":[]}`, http.StatusBadRequest},
		{"too many", `{"events":[` + strings.Repeat(goodEvent+",", 10) + goodEvent + `]}`, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, newHandler(&fakePublisher{}, fakeLimiter{allow: true}), tt.body)
			if rec.Code != tt.want {
				t.Fatalf("status %d, want %d (%s)", rec.Code, tt.want, rec.Body)
			}
		})
	}
}

func TestIngestRateLimited(t *testing.T) {
	rec := do(t, newHandler(&fakePublisher{}, fakeLimiter{allow: false}), `{"events":[`+goodEvent+`]}`)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("status %d retry-after %q", rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestIngestFailsOpenWhenLimiterDown(t *testing.T) {
	pub := &fakePublisher{}
	rec := do(t, newHandler(pub, fakeLimiter{err: errors.New("redis down")}), `{"events":[`+goodEvent+`]}`)
	if rec.Code != http.StatusAccepted || len(pub.got) != 1 {
		t.Fatalf("limiter outage must not drop events: status %d", rec.Code)
	}
}

func TestIngestPublishFailure(t *testing.T) {
	rec := do(t, newHandler(&fakePublisher{err: errors.New("kafka down")}, fakeLimiter{allow: true}), `{"events":[`+goodEvent+`]}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestRedisLimiter(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	lim := NewRedisLimiter(rdb, 10)
	ctx := context.Background()

	for attempt := 0; attempt < 3; attempt++ {
		mr.FlushAll()
		start := time.Now().Unix()
		ok1, _, err1 := lim.Allow(ctx, "k", 6)
		ok2, retry, err2 := lim.Allow(ctx, "k", 6)
		ok3, _, err3 := lim.Allow(ctx, "k", 4)
		if time.Now().Unix() != start {
			continue
		}
		if err1 != nil || err2 != nil || err3 != nil {
			t.Fatal(err1, err2, err3)
		}
		if !ok1 || ok2 || !ok3 {
			t.Fatalf("got %v %v %v, want true false true", ok1, ok2, ok3)
		}
		if retry <= 0 || retry > time.Second {
			t.Fatalf("retry-after %v out of range", retry)
		}
		if ok, _, _ := lim.Allow(ctx, "other", 10); !ok {
			t.Fatal("limits are per key")
		}
		return
	}
	t.Skip("clock kept rolling over; flaky environment")
}
