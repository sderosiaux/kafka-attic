#!/usr/bin/env bash
# Seed a Kafka-compatible cluster with the canonical kafka-attic e2e
# topic mix. Operates entirely through docker exec against the broker
# container so the host needs no Kafka tooling.
#
# Topics created (same set across every backend):
#   active-orders     — recent records + a healthy consumer group  (verdict: Active)
#   stale-events      — one record + no consumer                   (Inspect / Candidate)
#   empty-topic       — no records, no consumer                    (Candidate, APPEARS_NEVER_USED)
#   oversized-events  — 12 partitions, almost no traffic           (OVERSIZED flag)
#   compacted-state   — cleanup.policy=compact + a few records     (COMPACTED flag → INSPECT)
#
# Usage:
#   ./scripts/e2e/seed.sh redpanda
#   ./scripts/e2e/seed.sh kafka
#   ./scripts/e2e/seed.sh confluent
set -euo pipefail

BACKEND="${1:-}"
if [[ -z "$BACKEND" ]]; then
  echo "usage: $0 redpanda|kafka|confluent" >&2
  exit 2
fi

# Per-backend container name + Kafka-CLI command prefix + bootstrap (broker-internal).
case "$BACKEND" in
  redpanda)
    CONTAINER="kattic-e2e-redpanda"
    BOOTSTRAP_INTERNAL="redpanda:9092"
    # rpk is bundled in the Redpanda image; it talks the Kafka protocol.
    TOPICS_CMD="rpk topic"
    PRODUCE_CMD="rpk topic produce"
    GROUP_CMD="rpk group"
    CONFIG_FLAG="-c"  # rpk uses -c key=value for topic configs
    ;;
  kafka)
    CONTAINER="kattic-e2e-kafka"
    # bitnami/kafka uses 'kafka:9092' as the internal-network advertised name
    BOOTSTRAP_INTERNAL="kafka:9092"
    TOPICS_CMD="/opt/bitnami/kafka/bin/kafka-topics.sh"
    PRODUCE_CMD="/opt/bitnami/kafka/bin/kafka-console-producer.sh"
    GROUP_CMD="/opt/bitnami/kafka/bin/kafka-consumer-groups.sh"
    CONSOLE_CONSUMER="/opt/bitnami/kafka/bin/kafka-console-consumer.sh"
    ;;
  confluent)
    CONTAINER="kattic-e2e-cp-kafka"
    BOOTSTRAP_INTERNAL="cp-kafka:9092"
    TOPICS_CMD="kafka-topics"
    PRODUCE_CMD="kafka-console-producer"
    GROUP_CMD="kafka-consumer-groups"
    CONSOLE_CONSUMER="kafka-console-consumer"
    ;;
  *)
    echo "unknown backend: $BACKEND" >&2
    exit 2
    ;;
esac

dexec() { docker exec -i "$CONTAINER" "$@"; }

# Idempotent: delete the topic first so subsequent runs start clean.
# Errors here (e.g. topic not yet present) are intentionally swallowed.
delete_topic() {
  local name="$1"
  if [[ "$BACKEND" == "redpanda" ]]; then
    dexec rpk topic delete "$name" --brokers "$BOOTSTRAP_INTERNAL" >/dev/null 2>&1 || true
  else
    dexec $TOPICS_CMD --bootstrap-server "$BOOTSTRAP_INTERNAL" --delete --topic "$name" >/dev/null 2>&1 || true
  fi
}

create_topic() {
  local name="$1"
  local partitions="$2"
  shift 2
  local extra_configs=("$@")  # each formatted as "key=value"

  if [[ "$BACKEND" == "redpanda" ]]; then
    local args=(--brokers "$BOOTSTRAP_INTERNAL" create "$name" -p "$partitions" -r 1)
    for cfg in "${extra_configs[@]}"; do
      args+=(-c "$cfg")
    done
    dexec rpk topic "${args[@]}" >/dev/null
  else
    local args=(--bootstrap-server "$BOOTSTRAP_INTERNAL" --create --if-not-exists \
                --topic "$name" --partitions "$partitions" --replication-factor 1)
    for cfg in "${extra_configs[@]}"; do
      args+=(--config "$cfg")
    done
    dexec $TOPICS_CMD "${args[@]}" >/dev/null
  fi
  echo "  + topic $name (partitions=$partitions${extra_configs[*]:+, configs=${extra_configs[*]}})"
}

produce() {
  local topic="$1"
  local count="$2"
  local with_keys="${3:-false}"  # compacted topics require keys
  if [[ "$BACKEND" == "redpanda" ]]; then
    if [[ "$with_keys" == "true" ]]; then
      for i in $(seq 1 "$count"); do
        echo "key-$i:value-$i"
      done | dexec rpk topic produce "$topic" --brokers "$BOOTSTRAP_INTERNAL" -f '%k:%v\n' >/dev/null
    else
      for i in $(seq 1 "$count"); do
        echo "value-$i"
      done | dexec rpk topic produce "$topic" --brokers "$BOOTSTRAP_INTERNAL" -f '%v\n' >/dev/null
    fi
  else
    if [[ "$with_keys" == "true" ]]; then
      for i in $(seq 1 "$count"); do
        echo "key-$i:value-$i"
      done | dexec $PRODUCE_CMD --bootstrap-server "$BOOTSTRAP_INTERNAL" --topic "$topic" \
        --property "parse.key=true" --property "key.separator=:" >/dev/null 2>&1
    else
      for i in $(seq 1 "$count"); do
        echo "value-$i"
      done | dexec $PRODUCE_CMD --bootstrap-server "$BOOTSTRAP_INTERNAL" --topic "$topic" >/dev/null
    fi
  fi
  echo "  + $count record(s) → $topic"
}

# Run a short-lived consumer that commits offsets, then exits.
# Creates a stable consumer group entry in __consumer_offsets.
consume_to_commit() {
  local topic="$1"
  local group="$2"
  if [[ "$BACKEND" == "redpanda" ]]; then
    dexec rpk topic consume "$topic" --brokers "$BOOTSTRAP_INTERNAL" \
      --group "$group" --num 1 --offset start >/dev/null 2>&1 || true
  else
    timeout 5 docker exec -i "$CONTAINER" $CONSOLE_CONSUMER \
      --bootstrap-server "$BOOTSTRAP_INTERNAL" --topic "$topic" \
      --group "$group" --from-beginning --max-messages 1 --timeout-ms 4000 >/dev/null 2>&1 || true
  fi
  echo "  + consumer group $group committed offsets for $topic"
}

echo "[seed] backend=$BACKEND container=$CONTAINER"

echo "[seed] resetting any prior state"
for t in active-orders stale-events empty-topic oversized-events compacted-state; do
  delete_topic "$t"
done
# Give the broker a moment to commit deletions.
sleep 1

echo "[seed] creating topics"
create_topic active-orders    3
create_topic stale-events     1
create_topic empty-topic      1
create_topic oversized-events 12
create_topic compacted-state  1  "cleanup.policy=compact" "min.cleanable.dirty.ratio=0.01"

echo "[seed] producing records"
produce active-orders   25
produce stale-events     1
# empty-topic: no records on purpose
produce oversized-events 2
produce compacted-state  5 true   # compacted topics require keys

echo "[seed] committing a consumer group on active-orders"
consume_to_commit active-orders orders-consumer

echo "[seed] done"
