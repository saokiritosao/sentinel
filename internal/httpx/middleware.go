package httpx

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"sentinel/internal/config"
)

type ctxKey int

const (
	requestIDKey ctxKey = iota
	principalKey
)

type Middleware func(http.Handler) http.Handler

func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 64 {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sentinel_http_requests_total",
		Help: "HTTP requests by route and status code.",
	}, []string{"route", "code"})
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sentinel_http_request_duration_seconds",
		Help:    "HTTP request latency by route.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route"})
)

func Observe(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			// шаблон роута вместо пути, иначе в метриках будет метка на каждый id
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			elapsed := time.Since(start)
			httpRequests.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
			httpDuration.WithLabelValues(route).Observe(elapsed.Seconds())
			log.LogAttrs(r.Context(), slog.LevelInfo, "http request",
				slog.String("method", r.Method),
				slog.String("route", route),
				slog.Int("status", rec.status),
				slog.Duration("duration", elapsed),
				slog.String("request_id", RequestIDFrom(r.Context())),
			)
		})
	}
}

func Recover(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					if v == http.ErrAbortHandler {
						panic(v)
					}
					log.Error("panic in handler", "panic", v, "request_id", RequestIDFrom(r.Context()))
					Error(w, http.StatusInternalServerError, "internal", "internal error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func APIKeyAuth(keys []config.APIKey) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := []byte(r.Header.Get("X-API-Key"))
			name := ""
			for _, k := range keys {
				// сравнение за константное время, чтобы ключ нельзя было подобрать по таймингу
				if subtle.ConstantTimeCompare(got, []byte(k.Secret)) == 1 {
					name = k.Name
				}
			}
			if name == "" {
				Error(w, http.StatusUnauthorized, "unauthorized", "missing or invalid X-API-Key")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey, name)))
		})
	}
}

func PrincipalFrom(ctx context.Context) string {
	p, _ := ctx.Value(principalKey).(string)
	return p
}

func WithPrincipal(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, principalKey, name)
}
