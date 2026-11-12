package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sentinel/internal/alert"
)

type fakeStore struct {
	alerts     map[int64]alert.Alert
	lastFilter alert.Filter
	statsCalls int
}

func (s *fakeStore) List(_ context.Context, f alert.Filter) ([]alert.Alert, error) {
	s.lastFilter = f
	var out []alert.Alert
	for id := int64(100); id > 0 && len(out) < f.Limit; id-- {
		if a, ok := s.alerts[id]; ok && (f.Cursor == 0 || id < f.Cursor) {
			out = append(out, a)
		}
	}
	return out, nil
}

func (s *fakeStore) Get(_ context.Context, id int64) (alert.Alert, error) {
	a, ok := s.alerts[id]
	if !ok {
		return alert.Alert{}, alert.ErrNotFound
	}
	return a, nil
}

func (s *fakeStore) RecentEvents(context.Context, int64, int) ([]alert.EventRef, error) {
	return []alert.EventRef{}, nil
}

func (s *fakeStore) UpdateStatus(_ context.Context, id int64, to alert.Status) (alert.Alert, error) {
	a, ok := s.alerts[id]
	if !ok {
		return alert.Alert{}, alert.ErrNotFound
	}
	if !alert.CanTransition(a.Status, to) {
		return alert.Alert{}, fmt.Errorf("%w: %s -> %s", alert.ErrInvalidTransition, a.Status, to)
	}
	a.Status = to
	s.alerts[id] = a
	return a, nil
}

func (s *fakeStore) Stats(context.Context) (alert.Stats, error) {
	s.statsCalls++
	return alert.Stats{ByStatus: map[alert.Status]int{alert.StatusOpen: len(s.alerts)}}, nil
}

type memCache struct{ m map[string][]byte }

func (c *memCache) Get(_ context.Context, k string) ([]byte, bool, error) {
	b, ok := c.m[k]
	return b, ok, nil
}
func (c *memCache) Set(_ context.Context, k string, v []byte, _ time.Duration) error {
	c.m[k] = v
	return nil
}
func (c *memCache) Delete(_ context.Context, k string) error {
	delete(c.m, k)
	return nil
}

func setup(n int) (*fakeStore, *memCache, http.Handler) {
	store := &fakeStore{alerts: map[int64]alert.Alert{}}
	for i := 1; i <= n; i++ {
		store.alerts[int64(i)] = alert.Alert{ID: int64(i), Rule: "bruteforce", Status: alert.StatusOpen, Details: json.RawMessage(`{}`)}
	}
	cache := &memCache{m: map[string][]byte{}}
	h := NewHandler(store, cache, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	h.Register(mux, func(next http.Handler) http.Handler { return next })
	return store, cache, mux
}

func call(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func TestListPagination(t *testing.T) {
	_, _, h := setup(5)

	rec := call(h, "GET", "/v1/alerts?limit=2", "")
	var page listResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if rec.Code != 200 || len(page.Items) != 2 || page.Items[0].ID != 5 || page.NextCursor != "4" {
		t.Fatalf("page 1: %d %+v", rec.Code, page)
	}

	rec = call(h, "GET", "/v1/alerts?limit=2&cursor="+page.NextCursor, "")
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if len(page.Items) != 2 || page.Items[0].ID != 3 {
		t.Fatalf("page 2: %+v", page)
	}
}

func TestListValidation(t *testing.T) {
	_, _, h := setup(1)
	for _, q := range []string{"status=bogus", "severity=x", "limit=0", "limit=500", "cursor=-1"} {
		if rec := call(h, "GET", "/v1/alerts?"+q, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", q, rec.Code)
		}
	}
}

func TestGetAlert(t *testing.T) {
	_, _, h := setup(1)
	if rec := call(h, "GET", "/v1/alerts/1", ""); rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if rec := call(h, "GET", "/v1/alerts/42", ""); rec.Code != 404 {
		t.Fatalf("missing alert: status %d", rec.Code)
	}
	if rec := call(h, "GET", "/v1/alerts/abc", ""); rec.Code != 400 {
		t.Fatalf("bad id: status %d", rec.Code)
	}
}

func TestUpdateLifecycleAndCacheInvalidation(t *testing.T) {
	_, cache, h := setup(1)
	cache.m[statsCacheKey] = []byte(`{}`)

	if rec := call(h, "PATCH", "/v1/alerts/1", `{"status":"acknowledged"}`); rec.Code != 200 {
		t.Fatalf("ack: %d %s", rec.Code, rec.Body)
	}
	if _, ok := cache.m[statsCacheKey]; ok {
		t.Fatal("stats cache must be invalidated after a status change")
	}
	if rec := call(h, "PATCH", "/v1/alerts/1", `{"status":"resolved"}`); rec.Code != 200 {
		t.Fatalf("resolve: %d", rec.Code)
	}
	if rec := call(h, "PATCH", "/v1/alerts/1", `{"status":"open"}`); rec.Code != http.StatusConflict {
		t.Fatalf("reopening a resolved alert must be rejected, got %d", rec.Code)
	}
	if rec := call(h, "PATCH", "/v1/alerts/1", `{"status":"done"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown status: %d", rec.Code)
	}
}

func TestStatsCacheAside(t *testing.T) {
	store, _, h := setup(3)
	first := call(h, "GET", "/v1/stats", "")
	second := call(h, "GET", "/v1/stats", "")
	if first.Header().Get("X-Cache") != "MISS" || second.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("X-Cache: %q then %q", first.Header().Get("X-Cache"), second.Header().Get("X-Cache"))
	}
	if store.statsCalls != 1 {
		t.Fatalf("database queried %d times, want 1", store.statsCalls)
	}
	if first.Body.String() != second.Body.String() {
		t.Fatal("cached body differs")
	}
}

func TestCanTransition(t *testing.T) {
	cases := map[[2]alert.Status]bool{
		{alert.StatusOpen, alert.StatusAcknowledged}:     true,
		{alert.StatusOpen, alert.StatusResolved}:         true,
		{alert.StatusAcknowledged, alert.StatusResolved}: true,
		{alert.StatusAcknowledged, alert.StatusOpen}:     false,
		{alert.StatusResolved, alert.StatusOpen}:         false,
		{alert.StatusOpen, alert.StatusOpen}:             false,
	}
	for c, want := range cases {
		if got := alert.CanTransition(c[0], c[1]); got != want {
			t.Errorf("%s -> %s = %v, want %v", c[0], c[1], got, want)
		}
	}
}
