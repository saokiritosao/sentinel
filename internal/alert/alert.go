package alert

import (
	"encoding/json"
	"errors"
	"time"
)

type Severity string

const (
	SeverityLow      Severity = "low"
	SeverityMedium   Severity = "medium"
	SeverityHigh     Severity = "high"
	SeverityCritical Severity = "critical"
)

func (s Severity) Valid() bool {
	switch s {
	case SeverityLow, SeverityMedium, SeverityHigh, SeverityCritical:
		return true
	}
	return false
}

type Status string

const (
	StatusOpen         Status = "open"
	StatusAcknowledged Status = "acknowledged"
	StatusResolved     Status = "resolved"
)

func (s Status) Valid() bool {
	switch s {
	case StatusOpen, StatusAcknowledged, StatusResolved:
		return true
	}
	return false
}

var (
	ErrNotFound          = errors.New("alert not found")
	ErrInvalidTransition = errors.New("invalid status transition")
)

// open -> acknowledged -> resolved. resolved конечный, новая активность = новый алерт
func CanTransition(from, to Status) bool {
	switch from {
	case StatusOpen:
		return to == StatusAcknowledged || to == StatusResolved
	case StatusAcknowledged:
		return to == StatusResolved
	}
	return false
}

type Finding struct {
	Rule     string
	Severity Severity

	Entity  string
	Title   string
	Details map[string]any
}

func (f Finding) DedupKey() string { return f.Rule + ":" + f.Entity }

type Alert struct {
	ID         int64           `json:"id"`
	Rule       string          `json:"rule"`
	Severity   Severity        `json:"severity"`
	Entity     string          `json:"entity"`
	Title      string          `json:"title"`
	Status     Status          `json:"status"`
	EventCount int             `json:"event_count"`
	Details    json.RawMessage `json:"details"`
	FirstSeen  time.Time       `json:"first_seen"`
	LastSeen   time.Time       `json:"last_seen"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

type EventRef struct {
	EventID    string    `json:"event_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

type Filter struct {
	Status   Status
	Severity Severity
	Rule     string
	Entity   string

	Cursor int64
	Limit  int
}

type EntityCount struct {
	Entity     string `json:"entity"`
	Rule       string `json:"rule"`
	EventCount int    `json:"event_count"`
}

type Stats struct {
	ByStatus    map[Status]int   `json:"by_status"`
	BySeverity  map[Severity]int `json:"by_severity_active"`
	TopEntities []EntityCount    `json:"top_active_entities"`
	GeneratedAt time.Time        `json:"generated_at"`
}
