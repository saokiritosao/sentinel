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
	"github.com/segmentio/kafka-go"

	"sentinel/internal/config"
	"sentinel/internal/httpx"
	"sentinel/internal/ingest"
	"sentinel/internal/logx"
	"sentinel/internal/redisx"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ingest:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadIngest()
	if err != nil {
		return err
	}
	log := logx.New("ingest", cfg.Log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb, err := redisx.Connect(ctx, cfg.Redis, log)
	if err != nil {
		return err
	}
	defer rdb.Close()

	pub := ingest.NewKafkaPublisher(cfg.Kafka.Brokers, cfg.Kafka.EventsTopic)
	defer pub.Close()

	h := ingest.NewHandler(pub, ingest.NewRedisLimiter(rdb, cfg.RatePerSec), log, cfg.MaxBatch)

	mux := http.NewServeMux()
	mux.Handle("POST /v1/events", httpx.APIKeyAuth(cfg.APIKeys)(http.HandlerFunc(h.Ingest)))
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			httpx.Error(w, http.StatusServiceUnavailable, "not_ready", "redis: "+err.Error())
			return
		}
		conn, err := (&kafka.Dialer{}).DialContext(ctx, "tcp", cfg.Kafka.Brokers[0])
		if err != nil {
			httpx.Error(w, http.StatusServiceUnavailable, "not_ready", "kafka: "+err.Error())
			return
		}
		_ = conn.Close()
		w.WriteHeader(http.StatusOK)
	})

	handler := httpx.Chain(mux, httpx.RequestID, httpx.Observe(log), httpx.Recover(log))
	return httpx.Serve(ctx, httpx.NewServer(cfg.HTTPAddr, handler), log)
}
