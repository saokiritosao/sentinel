package detector

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"sentinel/internal/alert"
	"sentinel/internal/config"
	"sentinel/internal/event"
)

type Rule interface {
	Name() string
	Evaluate(ctx context.Context, ev event.Event) (*alert.Finding, error)
}

func DefaultRules(rdb redis.Cmdable, c config.Rules) []Rule {
	return []Rule{
		&BruteForce{rdb: rdb, threshold: c.BruteForceThreshold, window: c.BruteForceWindow},
		&PasswordSpray{rdb: rdb, threshold: c.SprayThreshold, window: c.SprayWindow, bucket: time.Minute},
		&NewCountry{rdb: rdb, ttl: c.GeoHistoryTTL},
	}
}

// брутфорс: sorted set на ip, score = время события, member = event_id
// (поэтому повторная доставка того же события не увеличит счётчик)
type BruteForce struct {
	rdb       redis.Cmdable
	threshold int
	window    time.Duration
}

func (r *BruteForce) Name() string { return "bruteforce" }

func (r *BruteForce) Evaluate(ctx context.Context, ev event.Event) (*alert.Finding, error) {
	if ev.Type != event.TypeAuthLogin || ev.Outcome != event.OutcomeFailure {
		return nil, nil
	}
	key := "rule:bf:" + ev.SourceIP
	ts := ev.Timestamp.UnixMilli()
	from := ts - r.window.Milliseconds()

	var count *redis.IntCmd

	// MULTI/EXEC, чтобы другая реплика не увидела окно наполовину обновлённым
	_, err := r.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZAdd(ctx, key, redis.Z{Score: float64(ts), Member: ev.ID})
		p.ZRemRangeByScore(ctx, key, "-inf", "("+strconv.FormatInt(from, 10))
		count = p.ZCount(ctx, key, strconv.FormatInt(from, 10), strconv.FormatInt(ts, 10))
		p.PExpire(ctx, key, 2*r.window)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("bruteforce window: %w", err)
	}

	n := int(count.Val())
	if n < r.threshold {
		return nil, nil
	}
	sev := alert.SeverityHigh
	if n >= 3*r.threshold {
		sev = alert.SeverityCritical
	}
	return &alert.Finding{
		Rule:     r.Name(),
		Severity: sev,
		Entity:   ev.SourceIP,
		Title:    fmt.Sprintf("Brute force: %d failed logins from %s in %s", n, ev.SourceIP, r.window),
		Details: map[string]any{
			"failed_attempts": n,
			"window_seconds":  int(r.window.Seconds()),
			"last_user":       ev.User,
		},
	}, nil
}

// спрей: HyperLogLog на (ip, минута). PFCOUNT по нескольким ключам
// возвращает кол-во уникальных юзеров за всё окно
type PasswordSpray struct {
	rdb       redis.Cmdable
	threshold int
	window    time.Duration
	bucket    time.Duration
}

func (r *PasswordSpray) Name() string { return "password_spray" }

func (r *PasswordSpray) Evaluate(ctx context.Context, ev event.Event) (*alert.Finding, error) {
	if ev.Type != event.TypeAuthLogin || ev.Outcome != event.OutcomeFailure {
		return nil, nil
	}
	cur := ev.Timestamp.UnixMilli() / r.bucket.Milliseconds()
	nBuckets := int64(r.window / r.bucket)
	keyFor := func(b int64) string { return "rule:spray:" + ev.SourceIP + ":" + strconv.FormatInt(b, 10) }

	keys := make([]string, 0, nBuckets)
	for b := cur - nBuckets + 1; b <= cur; b++ {
		keys = append(keys, keyFor(b))
	}

	var count *redis.IntCmd
	_, err := r.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.PFAdd(ctx, keyFor(cur), ev.User)
		p.PExpire(ctx, keyFor(cur), r.window+r.bucket)
		count = p.PFCount(ctx, keys...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("spray window: %w", err)
	}

	n := int(count.Val())
	if n < r.threshold {
		return nil, nil
	}
	return &alert.Finding{
		Rule:     r.Name(),
		Severity: alert.SeverityHigh,
		Entity:   ev.SourceIP,
		Title:    fmt.Sprintf("Password spraying: %s failed on %d distinct accounts in %s", ev.SourceIP, n, r.window),
		Details: map[string]any{
			"distinct_users": n,
			"window_seconds": int(r.window.Seconds()),
		},
	}, nil
}

// новая страна: hash user -> {страна: id первого события}.
// через HSETNX + проверку владельца, иначе при повторной доставке алерт мог потеряться
type NewCountry struct {
	rdb redis.Cmdable
	ttl time.Duration
}

func (r *NewCountry) Name() string { return "new_country" }

func (r *NewCountry) Evaluate(ctx context.Context, ev event.Event) (*alert.Finding, error) {
	if ev.Type != event.TypeAuthLogin || ev.Outcome != event.OutcomeSuccess {
		return nil, nil
	}
	key := "rule:geo:" + ev.User

	var (
		owner     *redis.StringCmd
		countries *redis.StringSliceCmd
	)
	_, err := r.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSetNX(ctx, key, ev.Country, ev.ID)
		owner = p.HGet(ctx, key, ev.Country)
		countries = p.HKeys(ctx, key)
		p.Expire(ctx, key, r.ttl)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("geo history: %w", err)
	}

	known := countries.Val()
	if owner.Val() != ev.ID || len(known) < 2 {
		return nil, nil
	}
	previous := make([]string, 0, len(known)-1)
	for _, c := range known {
		if c != ev.Country {
			previous = append(previous, c)
		}
	}
	return &alert.Finding{
		Rule:     r.Name(),
		Severity: alert.SeverityMedium,
		Entity:   ev.User,
		Title:    fmt.Sprintf("New country: %s logged in from %s for the first time", ev.User, ev.Country),
		Details: map[string]any{
			"country":            ev.Country,
			"src_ip":             ev.SourceIP,
			"previous_countries": previous,
		},
	}, nil
}
