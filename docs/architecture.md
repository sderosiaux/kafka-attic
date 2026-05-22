# Architecture of kafka-attic — Kafka topic cleanup pipeline

How kafka-attic collects Kafka topic facts, computes the ATTIC Score, and renders an auditable Kafka topic cleanup report — single static Go binary, read-only by construction.

This page summarises how kafka-attic is built. The authoritative source is [SPEC.md §5](/SPEC.md). Anything in this document that disagrees with the spec is wrong; raise an issue.

## Block diagram

```
┌──────────────┐
│  kafka-attic │  Single Go binary (franz-go client, no librdkafka, no producer)
└──────┬───────┘
       │
   ┌───┴──────────────────────────┐
   │  Cluster connection (read)   │
   │  • metadata + offsets        │  required
   │  • consumer group offsets    │  required
   │  • topic storage size        │  optional (often restricted on managed)
   │  • Confluent Schema Registry │  optional (Glue/Apicurio = v1.1)
   │  • Metrics (Prom / JMX)      │  optional enrichment
   │  • Owner mapping             │  optional (file / topic_config / Backstage / JSON)
   │  • Tiered storage probes     │  per cluster type
   └──────────┬───────────────────┘
              │
   ┌──────────▼──────────┐
   │  Scoring (ATTIC)    │
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   │  Renderer           │
   │  terminal / JSON / CSV / HTML
   └─────────────────────┘
```

## Layers

### Collection

The collector connects to the broker with [franz-go](https://github.com/twmb/franz-go) — a pure-Go Kafka client that supports SASL_PLAIN, SCRAM, mTLS, AWS IAM (MSK), and OAuth bearer without pulling in librdkafka. The collector only imports admin / consumer code paths; the producer is statically excluded at compile time and a CI test asserts this on every release.

For each topic in scope, the collector gathers:

- **Metadata** — partitions, replication factor, leader assignments. From `DescribeTopics`.
- **Offsets** — earliest and latest per partition, and the `LATEST_TIMESTAMP` per partition via `ListOffsets` with `timestamp = -1`. The maximum across partitions is the topic's last-produce timestamp. No record contents are ever fetched.
- **Consumer group state** — `DescribeConsumerGroups` for state + member count, `ListConsumerGroupOffsets` for committed offsets per partition. Lag is computed from the join with latest offsets.
- **Storage** *(optional)* — `DescribeLogDirs` for exact byte counts. When restricted (typical on Confluent Cloud, MSK Serverless), the collector falls back to a broker-reported average-record-size estimate or skips the Tonnage signal entirely.
- **Topic configs** — `DescribeConfigs` for `cleanup.policy`, `retention.ms`, `remote.storage.enable`, `confluent.placement.constraints`, and optional owner annotations.
- **Schema Registry** *(optional)* — Confluent SR client probes for subjects under the configured naming strategy (`topic_name`, `topic_record`).
- **Metrics** *(optional)* — Prometheus or JMX scrape for message rate, used to gate the `OVERSIZED` flag.
- **Owners** *(optional)* — file / topic config / Backstage / JSON endpoint, resolved by precedence.

The collector runs with bounded concurrency and exponential backoff on throttling responses, so it is safe on busy clusters. Permissions that fail are recorded in the JSON snapshot's `permissions_observed` block so the downstream user can see which signals were degraded by missing access vs missing data.

### Scoring

The scorer takes the collected per-topic facts and produces five sub-scores (Activity, Tenancy, Tonnage, Intent, Consumption), each in `[0, 100]` with an evidence level (`KNOWN`, `ESTIMATED`, `UNKNOWN`). It then:

1. Skips Tonnage / Intent when their evidence is `UNKNOWN`, redistributing weight proportionally across the remaining sub-signals.
2. For Activity / Tenancy / Consumption with `UNKNOWN` evidence, contributes a neutral 50 and adds the `MISSING_SIGNAL` flag.
3. Computes the weighted raw score.
4. Maps the raw score to a verdict band (`ACTIVE` / `INSPECT` / `CANDIDATE` / `LIKELY_UNUSED`).
5. Applies verdict caps for `ESTIMATED` evidence, `MISSING_SIGNAL`, `COMPACTED`, `REMOTE_STORAGE`, and `APPEARS_NEVER_USED` per the [ATTIC Score spec](/docs/attic-score-spec-v1.0.md).
6. Emits any applicable flags from the taxonomy (`PURGED`, `OVERSIZED`, `SKEWED`, `ORPHAN_SCHEMA`, `COMPACTED`, `REMOTE_STORAGE`, `APPEARS_NEVER_USED`, `MISSING_SIGNAL`).

The scorer is pure: same inputs produce the same outputs, no I/O, no clock dependency beyond what's in the inputs themselves. This is what makes the JSON snapshot reproducible across runs and machines.

### Render

The renderer produces four output formats from the same in-memory model:

- **Terminal** — human-labelled table with the headline reclaim line, the per-topic row, and the plain-English notes column. Default for `kattic scan`.
- **JSON** — canonical snapshot format consumed by `kattic diff` and by any downstream tooling. Schema versioned via `schema_version`; methodology versioned via `attic_spec_version`.
- **CSV** — flat-table projection of the JSON for spreadsheet use.
- **HTML** — single self-contained file with sortable tables, per-signal contribution view, reclaim summary, missing-signals notice, and the cleanup script section.

All four formats are derived from the same `Snapshot` struct; there is no scorer logic in the renderer.

### History (optional)

When `history.enabled: true`, the JSON snapshot is also persisted to a local SQLite database at `~/.kattic/history.db`. The history layer powers two things and nothing else:

- **Trend charts** in the HTML report when more than one prior scan is available.
- **`kattic diff`** — the diff command can either consume two on-disk JSON files directly or pick the two most recent snapshots out of history.

The scorer never reads from history. Scoring is fully determined by the current scan; history is purely an artefact archive. This is a deliberate design choice — it keeps the scoring spec implementable by any tool, with or without local state, and keeps a single-shot CLI run identical to a CI batch run.

## Related

- [SPEC.md](/SPEC.md) — full product spec
- [ATTIC Score spec v1.0](/docs/attic-score-spec-v1.0.md) — methodology, formulas, evidence model, flag taxonomy
- [kafka-attic vs AKHQ](/docs/vs/akhq.md), [kafka-attic vs Cruise Control](/docs/vs/cruise-control.md), [kafka-attic vs Confluent Health+](/docs/vs/confluent-health-plus.md) — neighbouring tools
- [README](/README.md) — project overview and install
- [Landing page](https://sderosiaux.github.io/kafka-attic/) — canonical home

---

Last updated: 2026-05-22
