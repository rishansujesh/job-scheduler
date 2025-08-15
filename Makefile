SHELL := /bin/bash

.PHONY: build test lint docker-up docker-down migrate e2e

build:
	go build ./...

test:
	go test -race ./...

lint:
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint not found. Install with:"; \
		echo "  curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/v1.59.1/install.sh | sh -s -- -b $(HOME)/bin v1.59.1"; \
		exit 1; \
	fi
	golangci-lint run ./...

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v

migrate:
	go run internal/db/migrate.go

e2e:
	@echo "Ensure stack is running: docker compose up -d"
	E2E=1 go test -v ./integration/...
