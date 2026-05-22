#!/usr/bin/env bash
# End-to-end smoke test of kafka-attic against a real Kafka-compatible
# broker. Supports three backends: redpanda, kafka (Apache KRaft),
# confluent (Confluent Community Edition).
#
# Usage:
#   ./scripts/e2e/run.sh                # default: redpanda
#   ./scripts/e2e/run.sh redpanda
#   ./scripts/e2e/run.sh kafka
#   ./scripts/e2e/run.sh confluent
#   ./scripts/e2e/run.sh all            # run against each backend sequentially
#
# Flags via env:
#   KEEP_RUNNING=1   skip teardown (leave containers up for inspection)
#   NO_BUILD=1       skip rebuilding the binary
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$REPO_ROOT/bin/kattic"

BACKENDS_ARG="${1:-redpanda}"

run_backend() {
  local backend="$1"
  local compose_file="$SCRIPT_DIR/compose/${backend}.yml"
  local config_file="$SCRIPT_DIR/configs/${backend}.yaml"

  if [[ ! -f "$compose_file" ]]; then
    echo "no compose file for backend '$backend' at $compose_file" >&2
    return 2
  fi
  if [[ ! -f "$config_file" ]]; then
    echo "no kattic config for backend '$backend' at $config_file" >&2
    return 2
  fi

  echo
  echo "================================================================"
  echo " kafka-attic e2e  ·  backend=$backend"
  echo "================================================================"

  trap "[[ -z \"\${KEEP_RUNNING:-}\" ]] && docker compose -f \"$compose_file\" -p kattic-e2e-$backend down -v >/dev/null 2>&1 || true" RETURN

  echo "[1/5] starting $backend"
  docker compose -f "$compose_file" -p "kattic-e2e-$backend" up -d --quiet-pull
  echo "[2/5] waiting for healthy"
  for i in {1..60}; do
    state=$(docker compose -f "$compose_file" -p "kattic-e2e-$backend" ps --format json 2>/dev/null | \
      python3 -c "
import sys,json
data=sys.stdin.read().strip()
if not data:
    print('starting'); sys.exit(0)
items=[json.loads(l) for l in data.splitlines() if l.strip()]
all_healthy=all(it.get('Health') in ('healthy','') and it.get('State')=='running' for it in items)
print('healthy' if all_healthy and items else 'starting')
")
    if [[ "$state" == "healthy" ]]; then
      echo "  ✓ $backend is healthy"
      break
    fi
    sleep 2
  done
  if [[ "$state" != "healthy" ]]; then
    echo "  ✗ $backend did not become healthy in time" >&2
    docker compose -f "$compose_file" -p "kattic-e2e-$backend" ps
    return 1
  fi

  echo "[3/5] seeding canonical topic mix"
  "$SCRIPT_DIR/seed.sh" "$backend"

  # Give the broker a moment to settle group state.
  sleep 2

  echo "[4/5] running kattic scan"
  "$BIN" scan --config "$config_file"

  echo "[5/5] asserting verdicts (json output)"
  json="$("$BIN" scan --config "$config_file" --format json)"

  # Use python to assert verdicts/flags on specific topics.
  if ! echo "$json" | python3 "$SCRIPT_DIR/assert.py" "$backend"; then
    echo "  ✗ assertions failed for $backend" >&2
    return 1
  fi
  echo "  ✓ $backend assertions passed"
}

main() {
  if [[ -z "${NO_BUILD:-}" ]]; then
    echo "[build] go build -o $BIN ./cmd/kattic"
    (cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/kattic)
  fi
  if [[ ! -x "$BIN" ]]; then
    echo "binary not built at $BIN" >&2
    exit 1
  fi

  case "$BACKENDS_ARG" in
    all)
      run_backend redpanda
      run_backend kafka
      run_backend confluent
      ;;
    redpanda|kafka|confluent)
      run_backend "$BACKENDS_ARG"
      ;;
    *)
      echo "usage: $0 redpanda|kafka|confluent|all" >&2
      exit 2
      ;;
  esac

  echo
  echo "================================================================"
  echo " all backends green"
  echo "================================================================"
}

main "$@"
