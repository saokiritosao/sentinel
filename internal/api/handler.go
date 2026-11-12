package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"sentinel/internal/alert"
	"sentinel/internal/httpx"
)

type Store interface {
	List(ctx context.Context, f alert.Filter) ([]alert.Alert, error)
	Get(ctx context.Context, id int64) (alert.Alert, error)
	RecentEvents(ctx context.Context, alertID int64, limit int) ([]alert.EventRef, error)
	UpdateStatus(ctx context.Context, id int64, to alert.Status) (alert.Alert, error)
	Stats(ctx context.Context) (alert.Stats, error)
}

type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

const (
	defaultLimit  = 50
	maxLimit      = 200
	statsCacheKey = "cache:api:stats:v1"
)

type Handler struct {
	store    Store
	cache    Cache
	statsTTL time.Duration
	log      *slog.Logger
}

func NewHandler(store Store, cache Cache, statsTTL time.Duration, log *slog.Logger) *Handler {
	return &Handler{store: store, cache: cache, statsTTL: statsTTL, log: log}
}

func (h *Handler) Register(mux *http.ServeMux, mw func(http.Handler) http.Handler) {
	mux.Handle("GET /v1/alerts", mw(http.HandlerFunc(h.listAlerts)))
	mux.Handle("GET /v1/alerts/{id}", mw(http.HandlerFunc(h.getAlert)))
	mux.Handle("PATCH /v1/alerts/{id}", mw(http.HandlerFunc(h.updateAlert)))
	mux.Handle("GET /v1/stats", mw(http.HandlerFunc(h.stats)))
}

type listResponse struct {
	Items      []alert.Alert `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

func (h *Handler) listAlerts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := alert.Filter{
		Status:   alert.Status(q.Get("status")),
		Severity: alert.Severity(q.Get("severity")),
		Rule:     q.Get("rule"),
		Entity:   q.Get("entity"),
		Limit:    defaultLimit,
	}
	if f.Status != "" && !f.Status.Valid() {
		httpx.Error(w, http.StatusBadRequest, "bad_request", "unknown status")
		return
	}
	if f.Severity != "" && !f.Severity.Valid() {
		httpx.Error(w, http.StatusBadRequest, "bad_request", "unknown severity")
		return
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxLimit {
			httpx.Error(w, http.StatusBadRequest, "bad_request", "limit must be 1..200")
			return
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			httpx.Error(w, http.StatusBadRequest, "bad_request", "invalid cursor")
			return
		}
		f.Cursor = n
	}

	items, err := h.store.List(r.Context(), f)
	if err != nil {
		h.internal(w, r, "list alerts", err)
		return
	}
	resp := listResponse{Items: items}
	if len(items) == f.Limit {
		resp.NextCursor = strconv.FormatInt(items[len(items)-1].ID, 10)
	}
	httpx.JSON(w, http.StatusOK, resp)
}

type alertDetail struct {
	alert.Alert
	RecentEvents []alert.EventRef `json:"recent_events"`
}

func (h *Handler) getAlert(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	a, err := h.store.Get(r.Context(), id)
	if errors.Is(err, alert.ErrNotFound) {
		httpx.Error(w, http.StatusNotFound, "not_found", "alert not found")
		return
	}
	if err != nil {
		h.internal(w, r, "get alert", err)
		return
	}
	events, err := h.store.RecentEvents(r.Context(), id, 20)
	if err != nil {
		h.internal(w, r, "recent events", err)
		return
	}
	httpx.JSON(w, http.StatusOK, alertDetail{Alert: a, RecentEvents: events})
}

type updateRequest struct {
	Status alert.Status `json:"status"`
}

func (h *Handler) updateAlert(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req updateRequest
	if err := httpx.DecodeJSON(w, r, &req, 1024); err != nil {
		httpx.Error(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if !req.Status.Valid() {
		httpx.Error(w, http.StatusBadRequest, "bad_request", "status must be open, acknowledged or resolved")
		return
	}

	a, err := h.store.UpdateStatus(r.Context(), id, req.Status)
	switch {
	case errors.Is(err, alert.ErrNotFound):
		httpx.Error(w, http.StatusNotFound, "not_found", "alert not found")
		return
	case errors.Is(err, alert.ErrInvalidTransition):
		httpx.Error(w, http.StatusConflict, "invalid_transition", err.Error())
		return
	case err != nil:
		h.internal(w, r, "update alert", err)
		return
	}

	h.log.InfoContext(r.Context(), "alert status changed",
		"alert_id", id, "status", a.Status, "by", httpx.PrincipalFrom(r.Context()))

	// статус поменялся -> сбрасываем кэш статистики
	if err := h.cache.Delete(r.Context(), statsCacheKey); err != nil {
		h.log.WarnContext(r.Context(), "stats cache invalidation failed", "err", err)
	}
	httpx.JSON(w, http.StatusOK, a)
}

// cache-aside: сначала redis, если нет - считаем в postgres и кладём с коротким TTL
func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if b, ok, err := h.cache.Get(ctx, statsCacheKey); err != nil {
		h.log.WarnContext(ctx, "stats cache read failed, using database", "err", err)
	} else if ok {
		w.Header().Set("X-Cache", "HIT")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
		return
	}

	s, err := h.store.Stats(ctx)
	if err != nil {
		h.internal(w, r, "stats", err)
		return
	}
	b, err := json.Marshal(s)
	if err != nil {
		h.internal(w, r, "marshal stats", err)
		return
	}
	if err := h.cache.Set(ctx, statsCacheKey, b, h.statsTTL); err != nil {
		h.log.WarnContext(ctx, "stats cache write failed", "err", err)
	}
	w.Header().Set("X-Cache", "MISS")
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func (h *Handler) internal(w http.ResponseWriter, r *http.Request, op string, err error) {
	h.log.ErrorContext(r.Context(), op+" failed", "err", err, "request_id", httpx.RequestIDFrom(r.Context()))
	httpx.Error(w, http.StatusInternalServerError, "internal", "internal error")
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		httpx.Error(w, http.StatusBadRequest, "bad_request", "id must be a positive integer")
		return 0, false
	}
	return id, true
}
