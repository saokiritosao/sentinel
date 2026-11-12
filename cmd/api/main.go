package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"sentinel/internal/api"
	"sentinel/internal/config"
	"sentinel/internal/httpx"
	"sentinel/internal/logx"
	"sentinel/internal/redisx"
	"sentinel/internal/storage/postgres"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI()
	if err != nil {
		return err
	}
	log := logx.New("api", cfg.Log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Connect(ctx, cfg.Postgres, log)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool, log); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	rdb, err := redisx.Connect(ctx, cfg.Redis, log)
	if err != nil {
		return err
	}
	defer rdb.Close()

	h := api.NewHandler(postgres.NewAlertRepository(pool), api.NewRedisCache(rdb), cfg.StatsTTL, log)

	mux := http.NewServeMux()
	h.Register(mux, httpx.APIKeyAuth(cfg.APIKeys))
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			httpx.Error(w, http.StatusServiceUnavailable, "not_ready", "postgres: "+err.Error())
			return
		}
		if err := rdb.Ping(ctx).Err(); err != nil {
			httpx.Error(w, http.StatusServiceUnavailable, "not_ready", "redis: "+err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := httpx.Chain(mux, httpx.RequestID, httpx.Observe(log), httpx.Recover(log))
	return httpx.Serve(ctx, httpx.NewServer(cfg.HTTPAddr, handler), log)
}
