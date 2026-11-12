package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"sentinel/internal/event"
	"sentinel/internal/httpx"
)

type Publisher interface {
	Publish(ctx context.Context, events []event.Event) error
}

type Limiter interface {
	Allow(ctx context.Context, key string, n int) (ok bool, retryAfter time.Duration, err error)
}

var eventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "sentinel_ingest_events_total",
	Help: "Events received by ingest, by result.",
}, []string{"result"})

type Handler struct {
	pub      Publisher
	lim      Limiter
	log      *slog.Logger
	maxBatch int
	now      func() time.Time
}

func NewHandler(pub Publisher, lim Limiter, log *slog.Logger, maxBatch int) *Handler {
	return &Handler{pub: pub, lim: lim, log: log, maxBatch: maxBatch, now: time.Now}
}

type batchRequest struct {
	Events []event.Event `json:"events"`
}

type fieldError struct {
	Index int    `json:"index"`
	Error string `json:"error"`
}

const maxReportedErrors = 20

func (h *Handler) Ingest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req batchRequest

	if err := httpx.DecodeJSON(w, r, &req, int64(h.maxBatch)*1024); err != nil {
		httpx.Error(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	n := len(req.Events)
	if n == 0 {
		httpx.Error(w, http.StatusBadRequest, "bad_request", "events must not be empty")
		return
	}
	if n > h.maxBatch {
		httpx.Error(w, http.StatusRequestEntityTooLarge, "batch_too_large",
			fmt.Sprintf("at most %d events per request", h.maxBatch))
		return
	}

	// если в пачке есть хоть одно плохое событие, не принимаем ничего
	now := h.now()
	var errs []fieldError
	for i, ev := range req.Events {
		if err := ev.Validate(now); err != nil && len(errs) < maxReportedErrors {
			errs = append(errs, fieldError{Index: i, Error: err.Error()})
		}
	}
	if len(errs) > 0 {
		eventsTotal.WithLabelValues("rejected").Add(float64(n))
		httpx.ErrorDetails(w, http.StatusUnprocessableEntity, "validation_failed",
			"batch contains invalid events; nothing was accepted", errs)
		return
	}

	principal := httpx.PrincipalFrom(ctx)
	ok, retryAfter, err := h.lim.Allow(ctx, principal, n)
	if err != nil {
		// redis упал -> пропускаем (fail open), потерять события безопасности хуже
		h.log.WarnContext(ctx, "rate limiter unavailable, failing open", "err", err)
	} else if !ok {
		eventsTotal.WithLabelValues("rate_limited").Add(float64(n))
		secs := int(retryAfter.Round(time.Second) / time.Second)
		w.Header().Set("Retry-After", strconv.Itoa(max(secs, 1)))
		httpx.Error(w, http.StatusTooManyRequests, "rate_limited", "event rate limit exceeded")
		return
	}

	if err := h.pub.Publish(ctx, req.Events); err != nil {
		h.log.ErrorContext(ctx, "publish failed", "err", err, "events", n,
			"request_id", httpx.RequestIDFrom(ctx))
		eventsTotal.WithLabelValues("publish_failed").Add(float64(n))
		w.Header().Set("Retry-After", "1")
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable", "event log unavailable, retry later")
		return
	}

	eventsTotal.WithLabelValues("accepted").Add(float64(n))
	httpx.JSON(w, http.StatusAccepted, map[string]int{"accepted": n})
}
