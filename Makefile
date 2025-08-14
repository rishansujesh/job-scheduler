SHELL := /bin/bash
PROJECT := job-scheduler

# Tool versions
GOLANGCI_LINT_VERSION ?= v1.60.1

.PHONY: help
help:
	@echo "Targets:"
	@echo "  build         Build all Go modules"
	@echo "  test          Run unit tests"
	@echo "  fmt           gofmt all packages"
	@echo "  vet           go vet all packages"
	@echo "  lint          Run golangci-lint"
	@echo "  docker-up     docker compose up -d --build"
	@echo "  docker-down   docker compose down -v"
	@echo "  migrate       Run DB migrations (go run internal/db/migrate.go)"
	@echo "  tools         Install local tools (golangci-lint)"

.PHONY: build
build:
	@echo ">> building..."
	go build ./...

.PHONY: test
test:
	@echo ">> testing..."
	go test -race -count=1 ./...

.PHONY: fmt
fmt:
	@echo ">> formatting..."
	gofmt -s -w .

.PHONY: vet
vet:
	@echo ">> vet..."
	go vet ./...

.PHONY: lint
lint: tools
	@echo ">> linting..."
	golangci-lint run ./...

.PHONY: docker-up
docker-up:
	@echo ">> docker compose up"
	docker compose up -d --build

.PHONY: docker-down
docker-down:
	@echo ">> docker compose down"
	docker compose down -v

.PHONY: migrate
migrate:
	@echo ">> running migrations..."
	go run internal/db/migrate.go

.PHONY: tools
tools:
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo ">> installing golangci-lint $(GOLANGCI_LINT_VERSION)"; \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | \
		  sh -s -- -b $$HOME/bin $(GOLANGCI_LINT_VERSION); \
		echo '>> add $$HOME/bin to your PATH if needed'; \
	fi
