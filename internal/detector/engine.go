package detector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"sentinel/internal/alert"
	"sentinel/internal/event"
)

type AlertStore interface {
	Record(ctx context.Context, f alert.Finding, eventID string, occurredAt time.Time) (alertID int64, created bool, err error)
}

var (
	findingsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sentinel_detector_findings_total",
		Help: "Findings produced, by rule.",
	}, []string{"rule"})
	alertsCreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sentinel_detector_alerts_created_total",
		Help: "New alerts opened, by rule.",
	}, []string{"rule"})
)

type Engine struct {
	rules        []Rule
	store        AlertStore
	rdb          redis.Cmdable
	processedTTL time.Duration
	log          *slog.Logger
}

func NewEngine(rules []Rule, store AlertStore, rdb redis.Cmdable, processedTTL time.Duration, log *slog.Logger) *Engine {
	return &Engine{rules: rules, store: store, rdb: rdb, processedTTL: processedTTL, log: log}
}

func (e *Engine) Handle(ctx context.Context, ev event.Event) (duplicate bool, err error) {
	// exactly-once тут невозможно (kafka, redis и postgres не в одной транзакции),
	// поэтому все шаги идемпотентные. маркер пишем в самом конце
	marker := "processed:" + ev.ID
	exists, err := e.rdb.Exists(ctx, marker).Result()
	if err != nil {
		return false, fmt.Errorf("check processed marker: %w", err)
	}
	if exists == 1 {
		return true, nil
	}

	for _, rule := range e.rules {
		f, err := rule.Evaluate(ctx, ev)
		if err != nil {
			return false, fmt.Errorf("rule %s: %w", rule.Name(), err)
		}
		if f == nil {
			continue
		}
		findingsTotal.WithLabelValues(f.Rule).Inc()

		id, created, err := e.store.Record(ctx, *f, ev.ID, ev.Timestamp)
		if err != nil {
			return false, fmt.Errorf("record %s finding: %w", f.Rule, err)
		}
		if created {
			alertsCreated.WithLabelValues(f.Rule).Inc()
			e.log.InfoContext(ctx, "alert opened",
				"alert_id", id, "rule", f.Rule, "severity", f.Severity, "entity", f.Entity)
		}
	}

	if err := e.rdb.Set(ctx, marker, 1, e.processedTTL).Err(); err != nil {
		// не критично: при повторе просто сделаем лишнюю (идемпотентную) работу
		e.log.WarnContext(ctx, "failed to write processed marker", "event_id", ev.ID, "err", err)
	}
	return false, nil
}
