package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"

	"sentinel/internal/config"
	"sentinel/internal/detector"
	"sentinel/internal/httpx"
	"sentinel/internal/logx"
	"sentinel/internal/redisx"
	"sentinel/internal/storage/postgres"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "detector:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadDetector()
	if err != nil {
		return err
	}
	log := logx.New("detector", cfg.Log)

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

	engine := detector.NewEngine(
		detector.DefaultRules(rdb, cfg.Rules),
		postgres.NewAlertRepository(pool),
		rdb, cfg.ProcessedTTL, log,
	)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Kafka.Brokers,
		GroupID: cfg.GroupID,
		Topic:   cfg.Kafka.EventsTopic,

		// новая группа читает с самого начала топика
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     500 * time.Millisecond,

		// 0 = без автокоммита, коммитим сами после обработки
		CommitInterval: 0,
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...any) {
			log.Error("kafka reader: " + fmt.Sprintf(msg, args...))
		}),
	})
	dlq := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Kafka.Brokers...),
		Topic:                  cfg.Kafka.DLQTopic,
		RequiredAcks:           kafka.RequireAll,
		BatchTimeout:           5 * time.Millisecond,
		AllowAutoTopicCreation: false,
	}

	consumer := detector.NewConsumer(reader, dlq, engine, log)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", readyz(pool.Ping, func(ctx context.Context) error { return rdb.Ping(ctx).Err() }))
	srv := httpx.NewServer(cfg.HTTPAddr, mux)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("consumer started", "topic", cfg.Kafka.EventsTopic, "group", cfg.GroupID)
		return consumer.Run(gctx)
	})
	g.Go(func() error {
		detector.ReportLag(gctx, reader, 5*time.Second)
		return nil
	})
	g.Go(func() error { return httpx.Serve(gctx, srv, log) })

	runErr := g.Wait()

	// закрываем reader, чтобы сразу выйти из группы и партиции ушли другой реплике
	if err := reader.Close(); err != nil {
		log.Warn("close reader", "err", err)
	}
	if err := dlq.Close(); err != nil {
		log.Warn("close dlq writer", "err", err)
	}
	log.Info("detector stopped")
	return runErr
}

func readyz(checks ...func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		for _, check := range checks {
			if err := check(ctx); err != nil {
				slog.WarnContext(ctx, "readiness check failed", "err", err)
				httpx.Error(w, http.StatusServiceUnavailable, "not_ready", err.Error())
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}
}
