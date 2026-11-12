package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"sentinel/internal/alert"
	"sentinel/internal/api"
	"sentinel/internal/config"
	"sentinel/internal/detector"
	"sentinel/internal/event"
	"sentinel/internal/httpx"
	"sentinel/internal/ingest"
	"sentinel/internal/simulator"
	"sentinel/internal/storage/postgres"
)

// kafka в тесте не поднимаем: топик в памяти с тем же форматом сообщений
type memTopic struct {
	mu        sync.Mutex
	cond      *sync.Cond
	msgs      []kafka.Message
	next      int
	committed int64
}

func newTopic() *memTopic {
	t := &memTopic{committed: -1}
	t.cond = sync.NewCond(&t.mu)
	return t
}

func (t *memTopic) Publish(_ context.Context, evs []event.Event) error {
	msgs, err := ingest.EncodeMessages(evs)
	if err != nil {
		return err
	}
	return t.WriteMessages(context.Background(), msgs...)
}

func (t *memTopic) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range msgs {
		m.Offset = int64(len(t.msgs))
		t.msgs = append(t.msgs, m)
	}
	t.cond.Broadcast()
	return nil
}

func (t *memTopic) FetchMessage(ctx context.Context) (kafka.Message, error) {
	stop := context.AfterFunc(ctx, func() {
		t.mu.Lock()
		t.cond.Broadcast()
		t.mu.Unlock()
	})
	defer stop()
	t.mu.Lock()
	defer t.mu.Unlock()
	for t.next >= len(t.msgs) {
		if ctx.Err() != nil {
			return kafka.Message{}, ctx.Err()
		}
		t.cond.Wait()
	}
	m := t.msgs[t.next]
	t.next++
	return m, nil
}

func (t *memTopic) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range msgs {
		t.committed = max(t.committed, m.Offset)
	}
	t.cond.Broadcast()
	return nil
}

func (t *memTopic) waitDrained(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		t.mu.Lock()
		done := t.committed == int64(len(t.msgs))-1
		t.mu.Unlock()
		if done {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestPipeline(t *testing.T) {
	dsn, redisAddr := os.Getenv("SENTINEL_TEST_POSTGRES_DSN"), os.Getenv("SENTINEL_TEST_REDIS_ADDR")
	if dsn == "" || redisAddr == "" {
		t.Skip("set SENTINEL_TEST_POSTGRES_DSN and SENTINEL_TEST_REDIS_ADDR to run the e2e test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	pool, err := postgres.Connect(ctx, config.Postgres{DSN: dsn, MaxConns: 5}, log)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool, log); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE alerts, alert_events"); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	topic, dlq := newTopic(), newTopic()
	repo := postgres.NewAlertRepository(pool)
	keys := []config.APIKey{{Name: "e2e", Secret: "e2e-secret-key-0123456789"}}

	ingestH := ingest.NewHandler(topic, ingest.NewRedisLimiter(rdb, 10_000), log, 500)
	ingestSrv := httptest.NewServer(httpx.APIKeyAuth(keys)(http.HandlerFunc(ingestH.Ingest)))
	defer ingestSrv.Close()

	rules := detector.DefaultRules(rdb, config.Rules{
		BruteForceThreshold: 10, BruteForceWindow: time.Minute,
		SprayThreshold: 5, SprayWindow: 5 * time.Minute,
		GeoHistoryTTL: time.Hour,
	})
	engine := detector.NewEngine(rules, repo, rdb, time.Hour, log)
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- detector.NewConsumer(topic, dlq, engine, log).Run(ctx) }()

	apiMux := http.NewServeMux()
	api.NewHandler(repo, api.NewRedisCache(rdb), time.Second, log).Register(apiMux, httpx.APIKeyAuth(keys))
	apiSrv := httptest.NewServer(apiMux)
	defer apiSrv.Close()

	gen := simulator.NewGenerator(42, 30)
	client := &simulator.Client{URL: ingestSrv.URL, APIKey: keys[0].Secret, HTTP: ingestSrv.Client()}
	now := time.Now().UTC().Truncate(time.Second)

	bf := gen.BruteForce(now)
	batches := [][]event.Event{
		gen.Baseline(now.Add(-time.Hour)),
		gen.Normal(200, now),
		bf,
		gen.Spray(now),
		gen.NewCountry(now),
		bf, // повторная отправка той же пачки, счётчик не должен задвоиться
	}
	sent := 0
	for _, b := range batches {
		if err := client.Send(ctx, b); err != nil {
			t.Fatalf("send: %v", err)
		}
		sent += len(b)
	}

	_ = topic.WriteMessages(ctx, kafka.Message{Value: []byte(`{"broken":`)})

	if !topic.waitDrained(10 * time.Second) {
		t.Fatal("consumer did not drain the topic in time")
	}

	var list struct {
		Items []alert.Alert `json:"items"`
	}
	get(t, apiSrv.URL+"/v1/alerts?limit=200", keys[0].Secret, &list)

	byRule := map[string][]alert.Alert{}
	for _, a := range list.Items {
		if a.Rule == "bruteforce" && a.Entity != bf[0].SourceIP {
			continue
		}
		byRule[a.Rule] = append(byRule[a.Rule], a)
	}
	if got := byRule["bruteforce"]; len(got) != 1 {
		t.Fatalf("want exactly one brute-force alert for %s, got %+v", bf[0].SourceIP, got)
	}

	if want := len(bf) - 10 + 1; byRule["bruteforce"][0].EventCount != want {
		t.Fatalf("brute-force event_count = %d, want %d (duplicates must not count)",
			byRule["bruteforce"][0].EventCount, want)
	}
	if len(byRule["password_spray"]) != 1 {
		t.Fatalf("want one spraying alert, got %+v", byRule["password_spray"])
	}
	if len(byRule["new_country"]) != 1 {
		t.Fatalf("want one new-country alert, got %+v", byRule["new_country"])
	}
	if len(dlq.msgs) != 1 {
		t.Fatalf("want 1 dead-lettered message, got %d", len(dlq.msgs))
	}

	id := byRule["bruteforce"][0].ID
	req, _ := http.NewRequest(http.MethodPatch, apiSrv.URL+"/v1/alerts/"+itoa(id), bytes.NewBufferString(`{"status":"resolved"}`))
	req.Header.Set("X-API-Key", keys[0].Secret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("resolve: %v %v", err, resp.Status)
	}
	resp.Body.Close()

	var stats alert.Stats
	get(t, apiSrv.URL+"/v1/stats", keys[0].Secret, &stats)
	if stats.ByStatus[alert.StatusResolved] != 1 {
		t.Fatalf("stats after resolve: %+v", stats)
	}

	cancel()
	if err := <-consumerDone; err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Logf("events sent: %d, alerts: %d, dlq: %d", sent, len(list.Items), len(dlq.msgs))
}

func get(t *testing.T, url, key string, dst any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-API-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, b)
	}
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		t.Fatal(err)
	}
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }
