.PHONY: build test integration e2e e2e-redpanda e2e-kafka e2e-confluent e2e-all lint lint-full fmt vet tidy vulncheck readonly-check verify clean dist install install-tools install-hooks uninstall-hooks help
.DEFAULT_GOAL := build

build: ## Build the kattic binary into ./bin
	go build -trimpath -ldflags="-s -w" -o ./bin/kattic ./cmd/kattic

test: ## Run unit tests with the race detector
	go test ./... -race -count=1

integration: ## Run integration tests (requires Docker)
	go test ./... -tags=integration -count=1 -timeout=15m

e2e: e2e-redpanda ## End-to-end smoke against Redpanda (default backend)

e2e-redpanda: ## End-to-end smoke against Redpanda (docker)
	bash scripts/e2e/run.sh redpanda

e2e-kafka: ## End-to-end smoke against Apache Kafka KRaft (docker)
	bash scripts/e2e/run.sh kafka

e2e-confluent: ## End-to-end smoke against Confluent Community Edition (docker)
	bash scripts/e2e/run.sh confluent

e2e-all: ## End-to-end smoke against all three backends sequentially
	bash scripts/e2e/run.sh all

lint: ## Run golangci-lint on the diff (fast)
	golangci-lint run --new-from-rev=origin/main --timeout=5m

lint-full: ## Run golangci-lint on the full tree
	golangci-lint run --timeout=10m

fmt: ## Format Go sources (gofumpt + goimports)
	@if command -v gofumpt >/dev/null 2>&1; then \
		gofumpt -w .; \
	else \
		gofmt -w .; \
	fi
	@if command -v goimports >/dev/null 2>&1; then \
		goimports -local github.com/sderosiaux/kafka-attic -w .; \
	fi

vet: ## Run go vet with all analyzers
	go vet ./...

tidy: ## Run go mod tidy and verify
	go mod tidy
	go mod verify

vulncheck: ## Run govulncheck across the module
	@command -v govulncheck >/dev/null 2>&1 || { echo "install with: go install golang.org/x/vuln/cmd/govulncheck@latest"; exit 1; }
	govulncheck ./...

readonly-check: ## Assert no producer code in non-test paths
	@! grep -RIn 'kgo.NewProducer\|\.ProduceSync(\|\.Produce(' --include="*.go" . | grep -v _test.go | grep -v 'ErrProduceForbidden\|LastProduceTs\|formatLastProduced\|lastProducedLabel\|MessageTimestampType\|callers must never invoke Produce\|LastProducedLabel\|//.*Produce' || { echo "READ-ONLY INVARIANT VIOLATED"; exit 1; }
	@echo "read-only invariant intact"

verify: tidy vet lint-full readonly-check test ## Run all the checks CI runs
	@echo "all checks passed"

clean: ## Remove build artifacts
	rm -rf ./bin /tmp/kattic dist/

dist: ## Build a snapshot release with goreleaser
	goreleaser release --snapshot --clean --skip=publish

install: ## Install kattic into $GOBIN / $GOPATH/bin
	go install ./cmd/kattic

install-tools: ## Install the dev tools (gofumpt, goimports, golangci-lint, govulncheck, lefthook)
	go install mvdan.cc/gofumpt@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	@if ! command -v lefthook >/dev/null 2>&1; then \
		echo "installing lefthook via go install (or brew install lefthook for a faster binary)"; \
		go install github.com/evilmartians/lefthook@latest; \
	fi

install-hooks: ## Install git pre-commit and pre-push hooks via lefthook
	@command -v lefthook >/dev/null 2>&1 || { echo "lefthook missing — run: make install-tools"; exit 1; }
	lefthook install
	@echo "hooks installed. To bypass once: LEFTHOOK=0 git commit ..."

uninstall-hooks: ## Remove the git hooks installed by lefthook
	@command -v lefthook >/dev/null 2>&1 && lefthook uninstall || true

help: ## Show available targets
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
