# kafka-attic e2e harness

Reproducible smoke test of `kafka-attic` against real Kafka-compatible brokers running on the local Docker daemon. Covers three backends:

| Backend | Image | Host port | Schema Registry |
|---|---|---|---|
| `redpanda` | `docker.redpanda.com/redpandadata/redpanda:v24.2.7` | `localhost:19092` | `localhost:18081` (bundled) |
| `kafka` | `bitnamilegacy/kafka:3.9.0` (Apache Kafka 3.9 KRaft) | `localhost:29094` | — |
| `confluent` | `confluentinc/cp-kafka:7.7.1` + `cp-schema-registry:7.7.1` (Confluent Community Edition) | `localhost:39092` | `localhost:38081` |

Each backend is seeded with the **canonical topic mix** so verdicts and flags can be asserted deterministically:

| Topic | Partitions | Records | Configs | Expected verdict / flag |
|---|---|---|---|---|
| `active-orders` | 3 | 25 | — | `ACTIVE` (or low `INSPECT`); consumer group `orders-consumer` committed |
| `stale-events` | 1 | 1 | — | not `LIKELY_UNUSED` (records present caps Consumption) |
| `empty-topic` | 1 | 0 | — | `APPEARS_NEVER_USED` flag |
| `oversized-events` | 12 | 2 | — | low traffic, lots of partitions |
| `compacted-state` | 1 | 5 keyed | `cleanup.policy=compact` | `COMPACTED` flag → verdict capped at `INSPECT` |

## Usage

```bash
# build + run a single backend
./scripts/e2e/run.sh redpanda
./scripts/e2e/run.sh kafka
./scripts/e2e/run.sh confluent

# all three backends sequentially
./scripts/e2e/run.sh all
```

Or via the Makefile:

```bash
make e2e            # default: redpanda
make e2e-kafka      # Apache Kafka KRaft
make e2e-confluent  # Confluent Community
make e2e-all        # all three
```

### Env-var knobs

| Variable | Effect |
|---|---|
| `KEEP_RUNNING=1` | Skip teardown; leave the container(s) up for manual inspection. Re-running with `KEEP_RUNNING` again uses the existing container (faster iteration). |
| `NO_BUILD=1` | Skip `go build`; reuse the binary already at `./bin/kattic`. |

### Manual teardown when `KEEP_RUNNING=1` was used

```bash
docker compose -f scripts/e2e/compose/redpanda.yml -p kattic-e2e-redpanda down -v
docker compose -f scripts/e2e/compose/kafka.yml -p kattic-e2e-kafka down -v
docker compose -f scripts/e2e/compose/confluent.yml -p kattic-e2e-confluent down -v
```

## What gets asserted

`scripts/e2e/assert.py` reads the JSON snapshot from `kattic scan --format json` and verifies, per backend:

- All 5 canonical topics are present in the scan
- `active-orders` is not `LIKELY_UNUSED` (it has records + an active consumer group)
- `empty-topic` carries `APPEARS_NEVER_USED`
- `compacted-state` carries `COMPACTED` and the verdict is capped at `INSPECT`
- `stale-events` is not `LIKELY_UNUSED` (records present)
- Every scanned topic's `raw_score` is in `[0, 100]`
- The snapshot's `attic_spec_version` is populated

The harness exits non-zero on the first failed assertion and dumps the offending `(topic, verdict, flags, raw_score)` tuple to stderr.

## Adding a new backend

1. Drop a `scripts/e2e/compose/<name>.yml` with a single Kafka-compatible service. Expose a unique host port (avoid `9092`, `19092`, `29092`, `29094`, `39092`).
2. Add `scripts/e2e/configs/<name>.yaml` matching the project's config schema (`internal/config/types.go`). Bootstrap should point at the host port; `auth.type: none` is fine.
3. Extend the `case` block in `seed.sh` with the container name, internal bootstrap address, and the Kafka CLI tooling path inside the image.
4. Register the backend in `run.sh`'s `case` block.

## Known quirks observed

- All three backends currently report `LAST PRODUCED: never seen` and a `MISSING_SIGNAL` flag for the Activity sub-signal even when records have been produced. This is a real `kafka-attic` bug — the `ListOffsets` timestamp probe is not extracting the LATEST timestamp correctly. The score / verdict / flag taxonomy still works because Consumption + Tonnage + Tenancy carry enough evidence; tracking this as an open issue.
- `bitnami/kafka` was renamed to `bitnamilegacy/kafka` upstream — the e2e pulls `bitnamilegacy/kafka:3.9.0`. The vanilla `apache/kafka:3.9.0` image rejects multi-listener configs that bind any listener on `0.0.0.0` even when the corresponding advertised listener is fine; the bitnami image's `KAFKA_CFG_*` translation handles this cleanly.
- Compacted topics must receive keyed records (`parse.key=true`, `key.separator=:`). The seed enables this for `compacted-state` only.
