package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	TypeAuthLogin = "auth.login"

	OutcomeSuccess = "success"
	OutcomeFailure = "failure"
)

var ErrInvalid = errors.New("invalid event")

type Event struct {
	ID        string    `json:"event_id"`
	Type      string    `json:"type"`
	Outcome   string    `json:"outcome"`
	User      string    `json:"user"`
	SourceIP  string    `json:"src_ip"`
	Country   string    `json:"country"`
	Source    string    `json:"source,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

const (
	maxFutureSkew = 5 * time.Minute
	maxAge        = 7 * 24 * time.Hour
)

// если now нулевой - время не проверяем (в detector событие может прийти повторно сильно позже)
func (e Event) Validate(now time.Time) error {
	if _, err := uuid.Parse(e.ID); err != nil {
		return invalid("event_id", "must be a UUID")
	}
	if e.Type != TypeAuthLogin {
		return invalid("type", "unsupported type %q", e.Type)
	}
	if e.Outcome != OutcomeSuccess && e.Outcome != OutcomeFailure {
		return invalid("outcome", "must be %q or %q", OutcomeSuccess, OutcomeFailure)
	}
	if e.User == "" || len(e.User) > 256 || !utf8.ValidString(e.User) {
		return invalid("user", "must be 1..256 bytes of valid UTF-8")
	}
	if _, err := netip.ParseAddr(e.SourceIP); err != nil {
		return invalid("src_ip", "must be an IPv4 or IPv6 address")
	}
	if !isCountryCode(e.Country) {
		return invalid("country", "must be an ISO 3166-1 alpha-2 code like US")
	}
	if len(e.Source) > 64 {
		return invalid("source", "must be at most 64 bytes")
	}
	if e.Timestamp.IsZero() {
		return invalid("timestamp", "is required")
	}
	if !now.IsZero() {
		if e.Timestamp.After(now.Add(maxFutureSkew)) {
			return invalid("timestamp", "is too far in the future")
		}
		if e.Timestamp.Before(now.Add(-maxAge)) {
			return invalid("timestamp", "is older than 7 days")
		}
	}
	return nil
}

func Decode(b []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return Event{}, fmt.Errorf("%w: decode json: %w", ErrInvalid, err)
	}
	if err := e.Validate(time.Time{}); err != nil {
		return Event{}, err
	}
	return e, nil
}

func isCountryCode(s string) bool {
	return len(s) == 2 && s[0] >= 'A' && s[0] <= 'Z' && s[1] >= 'A' && s[1] <= 'Z'
}

func invalid(field, format string, args ...any) error {
	return fmt.Errorf("%w: %s %s", ErrInvalid, field, fmt.Sprintf(format, args...))
}
