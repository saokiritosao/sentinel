package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sentinel/internal/event"
	"sentinel/internal/simulator"
)

func main() {
	var (
		url          = flag.String("url", envOr("SIM_INGEST_URL", "http://localhost:8080"), "ingest base URL")
		key          = flag.String("key", envOr("SIM_API_KEY", "dev-ingest-key-0123456789"), "ingest API key")
		rate         = flag.Int("rate", 50, "normal events per second")
		users        = flag.Int("users", 40, "number of simulated users")
		attackEvery  = flag.Duration("attack-every", 10*time.Second, "interval between attacks")
		duration     = flag.Duration("duration", 0, "how long to run (0 = until Ctrl+C)")
		seed         = flag.Uint64("seed", rand.Uint64(), "random seed")
		tickInterval = 500 * time.Millisecond
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	gen := simulator.NewGenerator(*seed, *users)
	client := &simulator.Client{URL: *url, APIKey: *key, HTTP: &http.Client{Timeout: 10 * time.Second}}

	if err := client.Send(ctx, gen.Baseline(time.Now())); err != nil {
		fmt.Fprintln(os.Stderr, "baseline:", err)
		os.Exit(1)
	}
	log.Info("baseline sent", "users", *users)

	attacks := []struct {
		name string
		gen  func(time.Time) []event.Event
	}{
		{"bruteforce", gen.BruteForce},
		{"password_spray", gen.Spray},
		{"new_country", gen.NewCountry},
	}

	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	attack := time.NewTicker(*attackEvery)
	defer attack.Stop()

	perTick := max(1, *rate*int(tickInterval.Milliseconds())/1000)
	sent, next := 0, 0
	for {
		select {
		case <-ctx.Done():
			log.Info("simulator stopped", "events_sent", sent)
			return
		case now := <-tick.C:
			batch := gen.Normal(perTick, now)
			if err := client.Send(ctx, batch); err != nil && ctx.Err() == nil {
				log.Error("send failed", "err", err)
				continue
			}
			sent += len(batch)
		case now := <-attack.C:
			a := attacks[next%len(attacks)]
			next++
			batch := a.gen(now)
			if err := client.Send(ctx, batch); err != nil && ctx.Err() == nil {
				log.Error("attack send failed", "attack", a.name, "err", err)
				continue
			}
			sent += len(batch)
			log.Info("attack injected", "attack", a.name, "events", len(batch),
				"src_ip", batch[0].SourceIP, "user", batch[0].User)
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
