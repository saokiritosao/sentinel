.PHONY: deps build test test-integration lint up down logs demo tools alerts stats clean

TEST_PG_DSN    ?= postgres://sentinel:sentinel@localhost:5432/sentinel?sslmode=disable
TEST_REDIS     ?= localhost:6379
INGEST_KEY     ?= dev-ingest-key-0123456789
API_KEY        ?= dev-api-key-0123456789

deps:
	go mod tidy

build:
	CGO_ENABLED=0 go build -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

test-integration:
	SENTINEL_TEST_POSTGRES_DSN="$(TEST_PG_DSN)" SENTINEL_TEST_REDIS_ADDR="$(TEST_REDIS)" \
		go test -race -count=1 -p 1 ./...

lint:
	golangci-lint run ./...

up: deps
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f ingest detector api

demo:
	docker compose --profile demo up -d --build simulator

tools:
	docker compose --profile tools up -d kafka-ui prometheus

alerts:
	@curl -s -H "X-API-Key: $(API_KEY)" "http://localhost:8081/v1/alerts?limit=10" | python3 -m json.tool

stats:
	@curl -s -i -H "X-API-Key: $(API_KEY)" http://localhost:8081/v1/stats

clean:
	rm -rf bin
