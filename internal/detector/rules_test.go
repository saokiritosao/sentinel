package detector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"sentinel/internal/alert"
	"sentinel/internal/event"
)

// miniredis - redis в памяти, для тестов правил docker не нужен
func newRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

var base = time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)

func login(user, ip, country, outcome string, ts time.Time) event.Event {
	return event.Event{
		ID: uuid.NewString(), Type: event.TypeAuthLogin, Outcome: outcome,
		User: user, SourceIP: ip, Country: country, Timestamp: ts,
	}
}

func TestBruteForceFiresAtThreshold(t *testing.T) {
	ctx := context.Background()
	r := &BruteForce{rdb: newRedis(t), threshold: 5, window: time.Minute}

	var f *alert.Finding
	for i := range 5 {
		var err error
		f, err = r.Evaluate(ctx, login("alice", "203.0.113.9", "NL", event.OutcomeFailure, base.Add(time.Duration(i)*time.Second)))
		if err != nil {
			t.Fatal(err)
		}
		if i < 4 && f != nil {
			t.Fatalf("fired too early at attempt %d", i+1)
		}
	}
	if f == nil {
		t.Fatal("expected finding at threshold")
	}
	if f.Entity != "203.0.113.9" || f.Severity != alert.SeverityHigh {
		t.Fatalf("unexpected finding %+v", f)
	}
}

func TestBruteForceIgnoresSuccessAndOldAttempts(t *testing.T) {
	ctx := context.Background()
	r := &BruteForce{rdb: newRedis(t), threshold: 3, window: time.Minute}

	for i := range 2 {
		_, _ = r.Evaluate(ctx, login("a", "198.51.100.1", "DE", event.OutcomeFailure, base.Add(time.Duration(i)*time.Second)))
	}

	_, _ = r.Evaluate(ctx, login("a", "198.51.100.1", "DE", event.OutcomeSuccess, base.Add(10*time.Minute)))

	f, err := r.Evaluate(ctx, login("a", "198.51.100.1", "DE", event.OutcomeFailure, base.Add(10*time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	if f != nil {
		t.Fatalf("should not fire, got %+v", f)
	}
}

func TestBruteForceIsIdempotent(t *testing.T) {
	ctx := context.Background()
	r := &BruteForce{rdb: newRedis(t), threshold: 2, window: time.Minute}
	ev := login("a", "198.51.100.2", "DE", event.OutcomeFailure, base)

	for range 3 {
		f, err := r.Evaluate(ctx, ev)
		if err != nil {
			t.Fatal(err)
		}
		if f != nil {
			t.Fatal("a redelivered event must not reach the threshold on its own")
		}
	}
}

func TestBruteForceEscalatesToCritical(t *testing.T) {
	ctx := context.Background()
	r := &BruteForce{rdb: newRedis(t), threshold: 2, window: time.Minute}
	var f *alert.Finding
	for i := range 6 {
		f, _ = r.Evaluate(ctx, login("a", "198.51.100.3", "DE", event.OutcomeFailure, base.Add(time.Duration(i)*time.Second)))
	}
	if f == nil || f.Severity != alert.SeverityCritical {
		t.Fatalf("want critical at 3x threshold, got %+v", f)
	}
}

func TestPasswordSprayCountsDistinctUsers(t *testing.T) {
	ctx := context.Background()
	r := &PasswordSpray{rdb: newRedis(t), threshold: 4, window: 5 * time.Minute, bucket: time.Minute}

	for i := range 10 {
		f, err := r.Evaluate(ctx, login("alice", "203.0.113.50", "VN", event.OutcomeFailure, base.Add(time.Duration(i)*time.Second)))
		if err != nil {
			t.Fatal(err)
		}
		if f != nil {
			t.Fatal("one user must not trigger spraying")
		}
	}

	var f *alert.Finding
	for i, u := range []string{"bob", "carol", "dave"} {
		f, _ = r.Evaluate(ctx, login(u, "203.0.113.50", "VN", event.OutcomeFailure, base.Add(time.Duration(i+1)*time.Minute)))
	}
	if f == nil {
		t.Fatal("expected spraying finding for 4 distinct users")
	}
	if got := f.Details["distinct_users"]; got != 4 {
		t.Fatalf("distinct_users = %v, want 4", got)
	}
}

func TestPasswordSprayWindowSlides(t *testing.T) {
	ctx := context.Background()
	r := &PasswordSpray{rdb: newRedis(t), threshold: 3, window: 2 * time.Minute, bucket: time.Minute}
	_, _ = r.Evaluate(ctx, login("u1", "203.0.113.51", "VN", event.OutcomeFailure, base))
	_, _ = r.Evaluate(ctx, login("u2", "203.0.113.51", "VN", event.OutcomeFailure, base))

	f, _ := r.Evaluate(ctx, login("u3", "203.0.113.51", "VN", event.OutcomeFailure, base.Add(10*time.Minute)))
	if f != nil {
		t.Fatal("old buckets must not count")
	}
}

func TestNewCountry(t *testing.T) {
	ctx := context.Background()
	r := &NewCountry{rdb: newRedis(t), ttl: time.Hour}

	first := login("alice", "198.51.100.10", "DE", event.OutcomeSuccess, base)
	if f, _ := r.Evaluate(ctx, first); f != nil {
		t.Fatal("first login is the baseline and must not alert")
	}
	if f, _ := r.Evaluate(ctx, login("alice", "198.51.100.11", "DE", event.OutcomeSuccess, base)); f != nil {
		t.Fatal("known country must not alert")
	}
	if f, _ := r.Evaluate(ctx, login("alice", "192.0.2.1", "BR", event.OutcomeFailure, base)); f != nil {
		t.Fatal("failed login from a new country is not an account compromise signal for this rule")
	}

	travel := login("alice", "192.0.2.1", "BR", event.OutcomeSuccess, base)
	f, err := r.Evaluate(ctx, travel)
	if err != nil {
		t.Fatal(err)
	}
	if f == nil || f.Entity != "alice" || f.Details["country"] != "BR" {
		t.Fatalf("expected new-country finding, got %+v", f)
	}

	again, _ := r.Evaluate(ctx, travel)
	if again == nil {
		t.Fatal("redelivered event must re-emit its finding")
	}

	if f, _ := r.Evaluate(ctx, login("alice", "192.0.2.2", "BR", event.OutcomeSuccess, base)); f != nil {
		t.Fatal("country seen before must not alert")
	}
}

func TestRulesAreIsolatedPerEntity(t *testing.T) {
	ctx := context.Background()
	r := &BruteForce{rdb: newRedis(t), threshold: 3, window: time.Minute}
	for i := range 6 {
		ip := fmt.Sprintf("198.51.100.%d", 100+i%3)
		if f, _ := r.Evaluate(ctx, login("a", ip, "DE", event.OutcomeFailure, base.Add(time.Duration(i)*time.Second))); f != nil {
			t.Fatalf("windows must be per IP, fired for %s", ip)
		}
	}
}
