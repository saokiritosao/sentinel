package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"sentinel/internal/event"
)

type Client struct {
	URL    string
	APIKey string
	HTTP   *http.Client
}

func (c *Client) Send(ctx context.Context, events []event.Event) error {
	body, err := json.Marshal(map[string]any{"events": events})
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/v1/events", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", c.APIKey)

		resp, err := c.HTTP.Do(req)
		if err != nil {
			return err
		}
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusAccepted:
			return nil
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			wait := time.Second
			if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				wait = time.Duration(s) * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		default:
			return fmt.Errorf("ingest returned %d: %s", resp.StatusCode, msg)
		}
	}
	return fmt.Errorf("giving up after retries")
}
