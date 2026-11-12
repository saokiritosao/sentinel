package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"sentinel/internal/alert"
)

type AlertRepository struct {
	pool *pgxpool.Pool
}

func NewAlertRepository(pool *pgxpool.Pool) *AlertRepository {
	return &AlertRepository{pool: pool}
}

// ON CONFLICT по частичному уникальному индексу (только не resolved алерты).
// (xmax = 0) -> строка была вставлена, а не обновлена
const upsertAlertSQL = `
INSERT INTO alerts (rule, severity, entity, dedup_key, title, details, first_seen, last_seen)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $7)
ON CONFLICT (dedup_key) WHERE status <> 'resolved' DO UPDATE SET
    last_seen  = GREATEST(alerts.last_seen, EXCLUDED.last_seen),
    first_seen = LEAST(alerts.first_seen, EXCLUDED.first_seen),
    title      = CASE WHEN EXCLUDED.last_seen >= alerts.last_seen THEN EXCLUDED.title   ELSE alerts.title   END,
    details    = CASE WHEN EXCLUDED.last_seen >= alerts.last_seen THEN EXCLUDED.details ELSE alerts.details END,
    severity   = CASE WHEN severity_rank(EXCLUDED.severity) > severity_rank(alerts.severity)
                      THEN EXCLUDED.severity ELSE alerts.severity END,
    updated_at = now()
RETURNING id, (xmax = 0) AS created`

// счётчик растёт только если пара (alert_id, event_id) новая, повтор не задвоит
const linkEventSQL = `
WITH ins AS (
    INSERT INTO alert_events (alert_id, event_id, occurred_at)
    VALUES ($1, $2::uuid, $3)
    ON CONFLICT DO NOTHING
    RETURNING 1
)
UPDATE alerts SET event_count = event_count + (SELECT count(*) FROM ins)
WHERE id = $1`

func (r *AlertRepository) Record(ctx context.Context, f alert.Finding, eventID string, occurredAt time.Time) (int64, bool, error) {
	details, err := json.Marshal(f.Details)
	if err != nil {
		return 0, false, fmt.Errorf("marshal details: %w", err)
	}

	var (
		id      int64
		created bool
	)
	err = pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, upsertAlertSQL,
			f.Rule, string(f.Severity), f.Entity, f.DedupKey(), f.Title, string(details), occurredAt,
		).Scan(&id, &created); err != nil {
			return fmt.Errorf("upsert alert: %w", err)
		}
		if _, err := tx.Exec(ctx, linkEventSQL, id, eventID, occurredAt); err != nil {
			return fmt.Errorf("link event: %w", err)
		}
		return nil
	})
	return id, created, err
}

const alertColumns = `id, rule, severity, entity, title, status, event_count, details,
	first_seen, last_seen, created_at, updated_at`

func scanAlert(row pgx.Row) (alert.Alert, error) {
	var a alert.Alert
	var details []byte
	err := row.Scan(&a.ID, &a.Rule, &a.Severity, &a.Entity, &a.Title, &a.Status, &a.EventCount,
		&details, &a.FirstSeen, &a.LastSeen, &a.CreatedAt, &a.UpdatedAt)
	a.Details = details
	return a, err
}

// keyset пагинация (id < cursor) вместо OFFSET, не тормозит на дальних страницах
func (r *AlertRepository) List(ctx context.Context, f alert.Filter) ([]alert.Alert, error) {
	var (
		conds []string
		args  []any
	)
	add := func(cond string, v any) {
		args = append(args, v)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != "" {
		add("status = $%d", string(f.Status))
	}
	if f.Severity != "" {
		add("severity = $%d", string(f.Severity))
	}
	if f.Rule != "" {
		add("rule = $%d", f.Rule)
	}
	if f.Entity != "" {
		add("entity = $%d", f.Entity)
	}
	if f.Cursor > 0 {
		add("id < $%d", f.Cursor)
	}

	q := "SELECT " + alertColumns + " FROM alerts"
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, f.Limit)
	q += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", len(args))

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]alert.Alert, 0, f.Limit)
	for rows.Next() {
		a, err := scanAlert(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (r *AlertRepository) Get(ctx context.Context, id int64) (alert.Alert, error) {
	a, err := scanAlert(r.pool.QueryRow(ctx, "SELECT "+alertColumns+" FROM alerts WHERE id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return alert.Alert{}, alert.ErrNotFound
	}
	return a, err
}

func (r *AlertRepository) RecentEvents(ctx context.Context, alertID int64, limit int) ([]alert.EventRef, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT event_id::text, occurred_at FROM alert_events
		WHERE alert_id = $1 ORDER BY occurred_at DESC LIMIT $2`, alertID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (alert.EventRef, error) {
		var e alert.EventRef
		err := row.Scan(&e.EventID, &e.OccurredAt)
		return e, err
	})
}

func (r *AlertRepository) UpdateStatus(ctx context.Context, id int64, to alert.Status) (alert.Alert, error) {
	var out alert.Alert
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		var from alert.Status
		// FOR UPDATE, чтобы два параллельных запроса не прошли проверку перехода
		err := tx.QueryRow(ctx, "SELECT status FROM alerts WHERE id = $1 FOR UPDATE", id).Scan(&from)
		if errors.Is(err, pgx.ErrNoRows) {
			return alert.ErrNotFound
		}
		if err != nil {
			return err
		}
		if !alert.CanTransition(from, to) {
			return fmt.Errorf("%w: %s -> %s", alert.ErrInvalidTransition, from, to)
		}
		out, err = scanAlert(tx.QueryRow(ctx,
			"UPDATE alerts SET status = $2, updated_at = now() WHERE id = $1 RETURNING "+alertColumns,
			id, string(to)))
		return err
	})
	return out, err
}

func (r *AlertRepository) Stats(ctx context.Context) (alert.Stats, error) {
	s := alert.Stats{
		ByStatus:    map[alert.Status]int{},
		BySeverity:  map[alert.Severity]int{},
		TopEntities: []alert.EntityCount{},
		GeneratedAt: time.Now().UTC(),
	}

	rows, err := r.pool.Query(ctx, "SELECT status, severity, count(*) FROM alerts GROUP BY status, severity")
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var (
			st  alert.Status
			sev alert.Severity
			n   int
		)
		if err := rows.Scan(&st, &sev, &n); err != nil {
			rows.Close()
			return s, err
		}
		s.ByStatus[st] += n
		if st != alert.StatusResolved {
			s.BySeverity[sev] += n
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return s, err
	}

	rows, err = r.pool.Query(ctx, `
		SELECT entity, rule, event_count FROM alerts
		WHERE status <> 'resolved'
		ORDER BY event_count DESC, id DESC LIMIT 10`)
	if err != nil {
		return s, err
	}
	s.TopEntities, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (alert.EntityCount, error) {
		var e alert.EntityCount
		err := row.Scan(&e.Entity, &e.Rule, &e.EventCount)
		return e, err
	})
	return s, err
}
