package postgres

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"sentinel/internal/alert"
	"sentinel/internal/config"
)

// интеграционные тесты, нужен SENTINEL_TEST_POSTGRES_DSN, иначе skip
func newRepo(t *testing.T) *AlertRepository {
	t.Helper()
	dsn := os.Getenv("SENTINEL_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SENTINEL_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool, err := Connect(ctx, config.Postgres{DSN: dsn, MaxConns: 10}, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := Migrate(ctx, pool, log); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE alerts, alert_events"); err != nil {
		t.Fatal(err)
	}
	return NewAlertRepository(pool)
}

func finding(entity string, sev alert.Severity, n int) alert.Finding {
	return alert.Finding{
		Rule: "bruteforce", Severity: sev, Entity: entity,
		Title: "test", Details: map[string]any{"failed_attempts": n},
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	repo := newRepo(t)
	if err := Migrate(context.Background(), repo.pool, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestRecordDeduplicatesAndIsIdempotent(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	t0 := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	ev1, ev2 := uuid.NewString(), uuid.NewString()

	id1, created, err := repo.Record(ctx, finding("203.0.113.1", alert.SeverityHigh, 10), ev1, t0)
	if err != nil || !created {
		t.Fatalf("first record: created=%v err=%v", created, err)
	}

	id2, created, err := repo.Record(ctx, finding("203.0.113.1", alert.SeverityHigh, 10), ev1, t0)
	if err != nil || created || id2 != id1 {
		t.Fatalf("redelivery: id=%d created=%v err=%v", id2, created, err)
	}

	_, _, err = repo.Record(ctx, finding("203.0.113.1", alert.SeverityCritical, 30), ev2, t0.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	a, err := repo.Get(ctx, id1)
	if err != nil {
		t.Fatal(err)
	}
	if a.EventCount != 2 {
		t.Fatalf("event_count = %d, want 2", a.EventCount)
	}
	if a.Severity != alert.SeverityCritical {
		t.Fatalf("severity = %s, want escalation to critical", a.Severity)
	}
	if !a.LastSeen.Equal(t0.Add(time.Second)) || !a.FirstSeen.Equal(t0) {
		t.Fatalf("first/last seen wrong: %v %v", a.FirstSeen, a.LastSeen)
	}

	_, _, _ = repo.Record(ctx, finding("203.0.113.1", alert.SeverityHigh, 1), uuid.NewString(), t0.Add(-time.Minute))
	a, _ = repo.Get(ctx, id1)
	if a.Severity != alert.SeverityCritical || !a.FirstSeen.Equal(t0.Add(-time.Minute)) {
		t.Fatalf("late event handled wrong: %+v", a)
	}
	if string(a.Details) != `{"failed_attempts": 30}` {
		t.Fatalf("details overwritten by older event: %s", a.Details)
	}

	events, err := repo.RecentEvents(ctx, id1, 10)
	if err != nil || len(events) != 3 {
		t.Fatalf("recent events: %v %v", events, err)
	}
}

func TestResolvedAlertIsNotReopened(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	id1, _, _ := repo.Record(ctx, finding("198.51.100.9", alert.SeverityHigh, 10), uuid.NewString(), t0)
	if _, err := repo.UpdateStatus(ctx, id1, alert.StatusResolved); err != nil {
		t.Fatal(err)
	}
	id2, created, err := repo.Record(ctx, finding("198.51.100.9", alert.SeverityHigh, 10), uuid.NewString(), t0.Add(time.Minute))
	if err != nil || !created || id2 == id1 {
		t.Fatalf("activity after resolution must open a new alert: id1=%d id2=%d created=%v err=%v", id1, id2, created, err)
	}
}

func TestUpdateStatusTransitions(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	id, _, _ := repo.Record(ctx, finding("198.51.100.10", alert.SeverityHigh, 10), uuid.NewString(), time.Now())

	if _, err := repo.UpdateStatus(ctx, id, alert.StatusAcknowledged); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.UpdateStatus(ctx, id, alert.StatusOpen); !errors.Is(err, alert.ErrInvalidTransition) {
		t.Fatalf("want ErrInvalidTransition, got %v", err)
	}
	if _, err := repo.UpdateStatus(ctx, 999999, alert.StatusResolved); !errors.Is(err, alert.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestConcurrentRecordsCreateOneAlert(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	t0 := time.Now().UTC()

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := repo.Record(ctx, finding("203.0.113.200", alert.SeverityHigh, 10), uuid.NewString(), t0.Add(time.Duration(i)*time.Millisecond))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	list, err := repo.List(ctx, alert.Filter{Entity: "203.0.113.200", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].EventCount != 20 {
		t.Fatalf("want one alert with 20 events, got %d alerts %+v", len(list), list)
	}
}

func TestListFiltersAndStats(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	for i, ip := range []string{"192.0.2.1", "192.0.2.2", "192.0.2.3"} {
		sev := alert.SeverityHigh
		if i == 2 {
			sev = alert.SeverityCritical
		}
		_, _, _ = repo.Record(ctx, finding(ip, sev, 10), uuid.NewString(), time.Now())
	}

	crit, err := repo.List(ctx, alert.Filter{Severity: alert.SeverityCritical, Limit: 10})
	if err != nil || len(crit) != 1 || crit[0].Entity != "192.0.2.3" {
		t.Fatalf("severity filter: %+v %v", crit, err)
	}
	page, _ := repo.List(ctx, alert.Filter{Limit: 2})
	rest, _ := repo.List(ctx, alert.Filter{Limit: 2, Cursor: page[1].ID})
	if len(page) != 2 || len(rest) != 1 {
		t.Fatalf("keyset pagination: %d then %d", len(page), len(rest))
	}

	s, err := repo.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.ByStatus[alert.StatusOpen] != 3 || s.BySeverity[alert.SeverityCritical] != 1 || len(s.TopEntities) != 3 {
		t.Fatalf("stats: %+v", s)
	}
}
