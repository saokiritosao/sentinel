package event

import (
	"errors"
	"testing"
	"time"
)

func valid(now time.Time) Event {
	return Event{
		ID:        "6f1c2c1e-6b1d-4a52-9d55-2f5c0f6b7a10",
		Type:      TypeAuthLogin,
		Outcome:   OutcomeFailure,
		User:      "alice@corp.example",
		SourceIP:  "203.0.113.7",
		Country:   "NL",
		Timestamp: now,
	}
}

func TestValidate(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		mutate  func(*Event)
		wantErr bool
	}{
		{"valid", func(*Event) {}, false},
		{"ipv6 is fine", func(e *Event) { e.SourceIP = "2001:db8::1" }, false},
		{"bad uuid", func(e *Event) { e.ID = "123" }, true},
		{"unknown type", func(e *Event) { e.Type = "file.open" }, true},
		{"bad outcome", func(e *Event) { e.Outcome = "maybe" }, true},
		{"empty user", func(e *Event) { e.User = "" }, true},
		{"bad ip", func(e *Event) { e.SourceIP = "999.1.1.1" }, true},
		{"lowercase country", func(e *Event) { e.Country = "nl" }, true},
		{"missing timestamp", func(e *Event) { e.Timestamp = time.Time{} }, true},
		{"far future", func(e *Event) { e.Timestamp = now.Add(time.Hour) }, true},
		{"too old", func(e *Event) { e.Timestamp = now.Add(-8 * 24 * time.Hour) }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := valid(now)
			tt.mutate(&e)
			err := e.Validate(now)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalid) {
				t.Fatalf("error should wrap ErrInvalid, got %v", err)
			}
		})
	}
}

func TestValidateSkipsTimeChecksWithZeroNow(t *testing.T) {
	e := valid(time.Now().Add(-30 * 24 * time.Hour))
	if err := e.Validate(time.Time{}); err != nil {
		t.Fatalf("old event must pass structural validation: %v", err)
	}
}

func TestDecode(t *testing.T) {
	if _, err := Decode([]byte("{not json")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid for malformed JSON, got %v", err)
	}
	ok := []byte(`{"event_id":"6f1c2c1e-6b1d-4a52-9d55-2f5c0f6b7a10","type":"auth.login",
		"outcome":"success","user":"bob","src_ip":"192.0.2.1","country":"DE",
		"timestamp":"2026-11-12T10:00:00Z"}`)
	ev, err := Decode(ok)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if ev.User != "bob" || ev.Country != "DE" {
		t.Fatalf("unexpected event %+v", ev)
	}
}
