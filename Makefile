.PHONY: build test integration lint fmt clean dist install help
.DEFAULT_GOAL := build

build: ## Build the kattic binary into ./bin
	go build -o ./bin/kattic ./cmd/kattic

test: ## Run unit tests with the race detector
	go test ./... -race -count=1

integration: ## Run integration tests (requires Docker)
	go test ./... -tags=integration -count=1 -timeout=15m

lint: ## Run golangci-lint
	golangci-lint run

fmt: ## Format Go sources (gofumpt if available, else gofmt)
	@if command -v gofumpt >/dev/null 2>&1; then \
		echo "gofumpt -w ."; \
		gofumpt -w .; \
	else \
		echo "gofumpt not found, falling back to gofmt"; \
		gofmt -w .; \
	fi

clean: ## Remove build artifacts
	rm -rf ./bin /tmp/kattic

dist: ## Build a snapshot release with goreleaser
	goreleaser release --snapshot --clean

install: ## Install kattic into $GOBIN / $GOPATH/bin
	go install ./cmd/kattic

help: ## Show available targets
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
