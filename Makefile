.PHONY: run test test-integration lint up down

run:
	go run ./cmd/api

test:
	go test -race -cover ./...

test-integration:
	go test -race -tags=integration ./...

lint:
	golangci-lint run

up:
	docker compose up --build

down:
	docker compose down -v
