package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Log struct {
	Level  slog.Level
	Format string
}

type Postgres struct {
	DSN      string
	MaxConns int32
}

type Redis struct {
	Addr     string
	Password string
	DB       int
}

type Kafka struct {
	Brokers     []string
	EventsTopic string
	DLQTopic    string
}

type APIKey struct {
	Name   string
	Secret string
}

type Ingest struct {
	Log        Log
	HTTPAddr   string
	APIKeys    []APIKey
	RatePerSec int
	MaxBatch   int
	Kafka      Kafka
	Redis      Redis
}

type Rules struct {
	BruteForceThreshold int
	BruteForceWindow    time.Duration
	SprayThreshold      int
	SprayWindow         time.Duration
	GeoHistoryTTL       time.Duration
}

type Detector struct {
	Log          Log
	HTTPAddr     string
	GroupID      string
	ProcessedTTL time.Duration
	Kafka        Kafka
	Redis        Redis
	Postgres     Postgres
	Rules        Rules
}

type API struct {
	Log      Log
	HTTPAddr string
	APIKeys  []APIKey
	StatsTTL time.Duration
	Redis    Redis
	Postgres Postgres
}

func LoadIngest() (Ingest, error) {
	e := &env{}
	c := Ingest{
		Log:        e.log(),
		HTTPAddr:   e.str("INGEST_HTTP_ADDR", ":8080"),
		APIKeys:    e.apiKeys("INGEST_API_KEYS"),
		RatePerSec: e.int("INGEST_RATE_PER_SEC", 2000),
		MaxBatch:   e.int("INGEST_MAX_BATCH", 500),
		Kafka:      e.kafka(),
		Redis:      e.redis(),
	}
	if c.MaxBatch > c.RatePerSec {
		e.errs = append(e.errs, errors.New("INGEST_MAX_BATCH must not exceed INGEST_RATE_PER_SEC"))
	}
	return c, e.err()
}

func LoadDetector() (Detector, error) {
	e := &env{}
	c := Detector{
		Log:          e.log(),
		HTTPAddr:     e.str("DETECTOR_HTTP_ADDR", ":9102"),
		GroupID:      e.str("DETECTOR_GROUP_ID", "sentinel-detector"),
		ProcessedTTL: e.dur("DETECTOR_PROCESSED_TTL", 24*time.Hour),
		Kafka:        e.kafka(),
		Redis:        e.redis(),
		Postgres:     e.postgres(),
		Rules: Rules{
			BruteForceThreshold: e.int("RULE_BRUTEFORCE_THRESHOLD", 10),
			BruteForceWindow:    e.dur("RULE_BRUTEFORCE_WINDOW", time.Minute),
			SprayThreshold:      e.int("RULE_SPRAY_THRESHOLD", 5),
			SprayWindow:         e.dur("RULE_SPRAY_WINDOW", 5*time.Minute),
			GeoHistoryTTL:       e.dur("RULE_GEO_HISTORY_TTL", 90*24*time.Hour),
		},
	}
	return c, e.err()
}

func LoadAPI() (API, error) {
	e := &env{}
	c := API{
		Log:      e.log(),
		HTTPAddr: e.str("API_HTTP_ADDR", ":8081"),
		APIKeys:  e.apiKeys("API_KEYS"),
		StatsTTL: e.dur("API_STATS_CACHE_TTL", 15*time.Second),
		Redis:    e.redis(),
		Postgres: e.postgres(),
	}
	return c, e.err()
}

// собираем все ошибки конфига сразу, а не падаем на первой
type env struct{ errs []error }

func (e *env) err() error { return errors.Join(e.errs...) }

func (e *env) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (e *env) int(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s: want positive integer, got %q", key, v))
		return def
	}
	return n
}

func (e *env) int32(key string, def int32) int32 {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s: want positive integer, got %q", key, v))
		return def
	}
	return int32(n)
}

func (e *env) dur(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s: want positive duration like 30s, got %q", key, v))
		return def
	}
	return d
}

func (e *env) list(key, def string) []string {
	var out []string
	for _, p := range strings.Split(e.str(key, def), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (e *env) log() Log {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(e.str("LOG_LEVEL", "info"))); err != nil {
		e.errs = append(e.errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	f := e.str("LOG_FORMAT", "json")
	if f != "json" && f != "text" {
		e.errs = append(e.errs, fmt.Errorf("LOG_FORMAT: want json or text, got %q", f))
	}
	return Log{Level: lvl, Format: f}
}

func (e *env) kafka() Kafka {
	return Kafka{
		Brokers:     e.list("KAFKA_BROKERS", "localhost:29092"),
		EventsTopic: e.str("KAFKA_EVENTS_TOPIC", "events.raw"),
		DLQTopic:    e.str("KAFKA_DLQ_TOPIC", "events.dlq"),
	}
}

func (e *env) redis() Redis {
	db, _ := strconv.Atoi(e.str("REDIS_DB", "0"))
	return Redis{
		Addr:     e.str("REDIS_ADDR", "localhost:6379"),
		Password: e.str("REDIS_PASSWORD", ""),
		DB:       db,
	}
}

func (e *env) postgres() Postgres {
	return Postgres{
		DSN:      e.str("POSTGRES_DSN", "postgres://sentinel:sentinel@localhost:5432/sentinel?sslmode=disable"),
		MaxConns: e.int32("POSTGRES_MAX_CONNS", 10),
	}
}

func (e *env) apiKeys(key string) []APIKey {
	raw := e.list(key, "")
	if len(raw) == 0 {
		e.errs = append(e.errs, fmt.Errorf("%s is required (format: name:secret,name2:secret2)", key))
		return nil
	}
	keys := make([]APIKey, 0, len(raw))
	for _, item := range raw {
		name, secret, ok := strings.Cut(item, ":")
		if !ok || name == "" || len(secret) < 16 {
			e.errs = append(e.errs, fmt.Errorf("%s: entry %q must be name:secret with secret >= 16 chars", key, name))
			continue
		}
		keys = append(keys, APIKey{Name: name, Secret: secret})
	}
	return keys
}
