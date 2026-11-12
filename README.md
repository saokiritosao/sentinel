# Sentinel

Real-time security event pipeline in Go. It receives login events over HTTP, detects attacks (brute force, password spraying, logins from a new country) and serves alerts through a REST API.

**Stack:** Go, Kafka, Redis, PostgreSQL, Docker Compose, Prometheus

```
ingest (HTTP) → Kafka → detector ×2 (rules in Redis) → PostgreSQL ← api (REST)
```

## Run

```bash
go mod tidy
docker compose up -d --build
docker compose --profile demo up -d simulator   # test traffic with attacks
```

```bash
curl -H "X-API-Key: dev-api-key-0123456789" localhost:8081/v1/alerts
```

Tests: `make test` (unit), `make test-integration` (needs the stack running).
