# kafka-attic

*Find stale, empty, oversized Kafka topics in one read-only scan.*

[![GitHub release](https://img.shields.io/github/v/release/conduktor/kafka-attic?display_name=tag&sort=semver)](https://github.com/conduktor/kafka-attic/releases)
[![CI](https://github.com/conduktor/kafka-attic/actions/workflows/ci.yml/badge.svg)](https://github.com/conduktor/kafka-attic/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/conduktor/kafka-attic)](https://goreportcard.com/report/github.com/conduktor/kafka-attic)
[![Homebrew](https://img.shields.io/badge/homebrew-conduktor%2Fkafka--attic-orange)](https://github.com/conduktor/homebrew-kafka-attic)
[![Made by Conduktor](https://img.shields.io/badge/made%20by-Conduktor-1f6feb)](https://conduktor.io?utm_source=kafka-attic&utm_medium=oss&utm_campaign=readme&utm_content=badge)

---

`kafka-attic` is a vendor-neutral CLI that scans an Apache Kafka cluster and surfaces topics that look stale, empty, oversized, or abandoned. Every topic gets an **ATTIC Score** (0-100) and a verdict band (Active / Inspect / Candidate / Likely unused) derived from a published, versioned methodology. The binary is read-only by construction: it does not write to the cluster, it does not read record bodies, and it cannot be compiled with a producer client.

Platform teams use kafka-attic to find their topic graveyard, reclaim storage, and prove what is safe to clean up. The output is auditable, the methodology is open, and the snapshot is reproducible across runs.

## What it does

- **Stale topics** — no producer activity for weeks or months, scored against a configurable activity curve (piecewise-linear, anchored at 30 / 90 / 180 / 365 days by default).
- **Empty topics** — `earliest_offset == latest_offset` across all partitions, with the difference between *never used* (`APPEARS_NEVER_USED`) and *records purged by retention* (`PURGED`) called out as separate, distinguishable flags.
- **Oversized / skewed topics** — partition counts that exceed throughput requirements, or single partitions carrying disproportionate load relative to siblings.
- **Schema-orphan topics** — Confluent Schema Registry has no subject targeting the topic under the configured naming strategy (`topic_name` or `topic_record`).

Three things kafka-attic deliberately does **not** do:

- It does not read record keys, values, or headers. Last-activity timestamps come from broker offset-by-timestamp APIs (`ListOffsets` with `timestamp = -1`), never from a `Fetch`.
- It does not mutate the cluster. The binary statically refuses to compile a producer client; a CI test asserts that no producer-capable code path is reachable.
- It is a single static Go binary — no JVM, no librdkafka, no agents to install on brokers. Runs from a laptop, a CI job, or a cron container with broker connectivity and read-only ACLs.

## Quick demo

<!-- TODO: replace with asciinema cast once recorded -->
<!--
<a href="https://asciinema.org/a/REPLACE_ME" target="_blank">
  <img src="https://asciinema.org/a/REPLACE_ME.svg" alt="kafka-attic demo" width="720"/>
</a>
-->

```
$ kattic scan --cluster prod.yaml

4,821 topics · 1,207 likely unused · 38.2 TB reclaimable

TOPIC              LAST PRODUCED  STORAGE     SCORE  VERDICT      NOTES
─────────────────  ─────────────  ──────────  ─────  ───────────  ─────────────────────────────────────
legacy-events      287d ago       12.3 GB     72     Candidate    no schema reference found
orders-v1          45d ago        2.1 GB      61     Inspect      partition load uneven
audit-trail        3d ago         890 MB      12     Active
empty-topic        never seen     0 B         88     Candidate    appears never used (low evidence)
old-events         180d ago       0 B         85     Candidate    records purged by retention
oversized-events   2h ago         412 GB      55     Inspect      over-provisioned partitions; partition load uneven
compacted-state    1d ago         5.2 GB est  42     Inspect      compacted topic; manual review required
remote-archive     90d ago        ? GB        —      —            tiered storage; storage unknown
```

The first line — `N topics · M likely unused · X TB reclaimable` — is the quotable summary every scan prints. It is the same number across terminal, JSON, CSV, and HTML outputs.

## Install

**Homebrew (macOS / Linux)**

```bash
brew install conduktor/kafka-attic/kafka-attic
```

**Scoop (Windows)**

```powershell
scoop bucket add conduktor https://github.com/conduktor/scoop-bucket
scoop install kafka-attic
```

**Docker**

```bash
docker pull ghcr.io/conduktor/kafka-attic:latest
docker run --rm -v "$PWD:/work" ghcr.io/conduktor/kafka-attic scan --cluster /work/prod.yaml
```

**curl one-liner**

```bash
curl -sSL https://github.com/conduktor/kafka-attic/releases/latest/download/install.sh | sh
```

**Go install**

```bash
go install github.com/conduktor/kafka-attic/cmd/kattic@latest
```

Release binaries are reproducible: every GitHub release ships SLSA-style provenance and a SHA-256 checksum file. The Homebrew formula and Scoop manifest are generated from those checksums. The Docker image is multi-arch (`linux/amd64`, `linux/arm64`) and runs as a non-root user with no shell.

## 30-second quickstart

Minimal `kattic.yaml`:

```yaml
cluster:
  name: prod
  bootstrap: broker-1.kafka.acme.com:9092
  auth:
    type: scram
    mechanism: SCRAM-SHA-512
    username_env: KAFKA_USER
    password_env: KAFKA_PASS
```

Run a scan:

```bash
kattic scan --cluster kattic.yaml
```

For an HTML report with the per-signal contribution view, the reclaim summary, and the cleanup script section:

```bash
kattic audit --cluster kattic.yaml --output attic-report.html
```

For a single-topic deep dive that prints the full per-signal breakdown, evidence levels, flags, and recent consumer-group state:

```bash
kattic inspect --topic legacy-events --cluster kattic.yaml
```

For a week-over-week reclaim diff between two stored snapshots:

```bash
kattic diff scans/2026-05-14.json scans/2026-05-21.json
```

<!-- TODO: insert screenshot of terminal output or HTML report hero -->

The `audit` command writes a single self-contained HTML file (no external assets, no CDN dependency, no JavaScript bundlers — vanilla `<script>` + inline SVG only) suitable for archival in an artifact store, attaching to a ticket, or emailing to a stakeholder who is not on the engineering Slack.

## What you get

The headline line above the table is the quotable summary every scan prints. It rolls up three numbers: topics scanned, topics in the `LIKELY_UNUSED` verdict band, and total reclaimable bytes across that band.

```
4,821 topics · 1,207 likely unused · 38.2 TB reclaimable

TOPIC              LAST PRODUCED  STORAGE     SCORE  VERDICT      NOTES
─────────────────  ─────────────  ──────────  ─────  ───────────  ─────────────────────────────────────
legacy-events      287d ago       12.3 GB     72     Candidate    no schema reference found
orders-v1          45d ago        2.1 GB      61     Inspect      partition load uneven
audit-trail        3d ago         890 MB      12     Active
empty-topic        never seen     0 B         88     Candidate    appears never used (low evidence)
old-events         180d ago       0 B         85     Candidate    records purged by retention
oversized-events   2h ago         412 GB      55     Inspect      over-provisioned partitions; partition load uneven
compacted-state    1d ago         5.2 GB est  42     Inspect      compacted topic; manual review required
remote-archive     90d ago        ? GB        —      —            tiered storage; storage unknown
```

Reading the table column by column:

- **TOPIC** — topic name as the broker reports it. When `report.redact_topic_names: hash` is set, JSON / CSV / shared artifacts substitute the SHA-256 digest, but the local terminal view keeps the real name.
- **LAST PRODUCED** — human time-delta since the most recent `LogAppendTime` (or `CreateTime`) observed across partitions. `never seen` means the topic has no records and no committed offsets from any group.
- **STORAGE** — exact bytes from `DescribeLogDirs` when available; otherwise `est` (estimated from offsets × broker-reported average record size), or `? GB` when storage cannot be determined (typical on Confluent Cloud / MSK Serverless / tiered storage).
- **SCORE** — the ATTIC Score from 0 to 100. Higher = stronger evidence of disuse. `—` is printed when scoring was skipped because of `REMOTE_STORAGE` with `UNKNOWN` Tonnage.
- **VERDICT** — `Active`, `Inspect`, `Candidate`, or `Likely unused`. Reflects the verdict cap; the numeric score is shown unmodified.
- **NOTES** — plain-English flag descriptions, comma-joined. JSON / CSV outputs keep the machine enums (`ORPHAN_SCHEMA`, `MISSING_SIGNAL`, `REMOTE_STORAGE`, ...) intact.

The four subcommands cover the full surface area:

- `kattic scan` — quick terminal scan; the fastest path to the headline reclaim number. Default output is the human table; `--format json` and `--format csv` switch to machine output.
- `kattic audit` — full audit; writes JSON + HTML + (when `--print-cleanup`) the cleanup script section. Saves a JSON snapshot under `~/.kattic/history.db` when history is enabled.
- `kattic inspect --topic <name>` — single-topic deep dive; prints each sub-signal's score, evidence level, raw input, and any flag. Used during cleanup review or when defending a score to a topic owner.
- `kattic diff <a.json> <b.json>` — week-over-week tracking; computes verdict transitions, reclaimed bytes between snapshots, and new candidates. Verifies that both snapshots share the same `attic_spec_version` before reporting numeric deltas.

## How the score works

Each topic receives an **ATTIC Score** from 0 to 100. Higher means stronger evidence of disuse. The score is *not* a probability — it is a weighted heuristic over five sub-signals, each independently scored in `[0, 100]` and tagged with an evidence level:

- **A — Activity** *(default weight 0.30)* — days since the most recent record on the topic. Sourced from `ListOffsets` with `timestamp = -1` per partition. Piecewise-linear, anchored at 0 / 30 / 90 / 180 / 365 days. Evidence is `KNOWN` when `message.timestamp.type = LogAppendTime` (broker-set, cannot lie), `ESTIMATED` when `CreateTime` (client-supplied, can lie), `UNKNOWN` for very old brokers.
- **T — Tenancy** *(default weight 0.20)* — state of consumer groups targeting the topic. Derived from `DescribeConsumerGroups` and `ListConsumerGroupOffsets`. Cascading rules: any `Stable` group with members → 0; an `Empty` group with lag → 50; all `Dead` or no groups → 100.
- **T — Tonnage** *(default weight 0.10)* — storage footprint percentile against the cluster; smaller topics score higher. `KNOWN` from `DescribeLogDirs`, `ESTIMATED` from offsets × broker-reported average record size, `UNKNOWN` on managed offerings where log-dir is locked down. When Tonnage is `UNKNOWN`, the sub-signal is **skipped** and its weight is redistributed proportionally across the rest.
- **I — Intent** *(default weight 0.15)* — whether a Schema Registry subject targets this topic (Confluent SR in v1; Glue and Apicurio planned for v1.1). Computed for `topic_name` and `topic_record` strategies. `record_name` strategy is unresolvable from SR alone and is skipped with weight redistribution.
- **C — Consumption** *(default weight 0.25)* — current record presence on the topic. `100` when `earliest == latest` across all partitions, `90` when records existed and were purged by retention (`earliest > 0` and `earliest == latest`), `0` when records are present.

**Verdict bands**: `LIKELY_UNUSED` (≥ 90), `CANDIDATE` (70-89), `INSPECT` (40-69), `ACTIVE` (< 40). **Verdict caps** apply when evidence is weak: any `ESTIMATED` evidence caps at `CANDIDATE`; missing Activity / Tenancy / Consumption caps at `INSPECT`; the `COMPACTED` and `REMOTE_STORAGE` flags also cap at `INSPECT`; `APPEARS_NEVER_USED` without `PURGED` evidence caps at `CANDIDATE`. The numeric score is printed unchanged — the cap only constrains the verdict label.

The full methodology, including formulas, evidence-level transitions, weight-redistribution math, the full flag taxonomy, and a worked example, is published as a versioned spec under [Creative Commons BY 4.0](https://creativecommons.org/licenses/by/4.0/) so other Kafka tooling can adopt the methodology independently of any single implementation.

Read the full methodology: [docs/attic-score-spec-v1.0.md](docs/attic-score-spec-v1.0.md).

## Managed Kafka support

kafka-attic works against any Apache Kafka cluster reachable over TCP, including the major managed offerings. The matrix below describes what evidence is available for the Tonnage sub-signal — every other sub-signal is collected from baseline admin APIs that all conformant Kafka clusters expose.

| Cluster type        | Tonnage evidence       | Notes                                                                                                                |
|---------------------|------------------------|----------------------------------------------------------------------------------------------------------------------|
| Self-managed Kafka  | `KNOWN`                | Full `DescribeLogDirs` support. Tonnage uses exact bytes per partition.                                              |
| MSK Provisioned     | `KNOWN`                | Requires IAM `DescribeClusterV2` and `kafka-cluster:DescribeTopicDynamicConfiguration`. Log-dir works on most plans. |
| MSK Serverless      | `UNKNOWN`              | Log-dir is restricted; Tonnage skipped and its weight redistributed across the other four sub-signals.                |
| Confluent Cloud     | `UNKNOWN`              | Log-dir restricted; `REMOTE_STORAGE` flag emitted when tiered storage or infinite retention (`retention.ms = -1`) is detected. |
| Aiven               | `KNOWN` or `ESTIMATED` | Varies by plan; some plans expose log-dir, others surface segment metadata only.                                      |
| Redpanda            | `KNOWN`                | Full log-dir support; Redpanda implements the Kafka admin API faithfully.                                            |

When Tonnage cannot be measured, its weight is redistributed proportionally across the other sub-signals — the score is still computed, just from one fewer signal, and the JSON snapshot records `"tonnage": { "evidence": "UNKNOWN" }` so downstream tooling can detect the degradation. The same redistribution applies to Intent when Schema Registry is not configured or the topic uses the `record_name` strategy.

Authentication coverage at v1.0: SASL_PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, mTLS, AWS IAM (MSK), OAuth bearer. Each auth type is configured under `cluster.auth.type` in `kattic.yaml`; sensitive material is sourced from environment variables (`*_env` keys) so the config file itself is safe to commit.

## Privacy

> **kafka-attic does not fetch record contents.** Last-activity timestamps come from broker offset-by-timestamp APIs (`ListOffsets` with timestamp `-1`). No record keys, values, or headers are read at any point. The binary statically refuses to compile a `Fetch`-capable code path; a CI test asserts this on every release.

The Tonnage estimate used on managed Kafka (`ESTIMATED` evidence) is computed from `(latest_offset − earliest_offset) × broker-reported average record size from log segment metadata`. The average is published by the broker itself; kafka-attic never samples records, never decodes a payload, and never opens a `Fetch` connection.

For environments where topic names themselves are sensitive (regulated industries, multi-tenant SaaS where topic names encode customer IDs or jurisdictions), set `report.redact_topic_names: hash` in `kattic.yaml`. Topic names, consumer group names, and Schema Registry subject names in the JSON / CSV / shared `audit --share` artifacts are then replaced by their SHA-256 digest. The local terminal output and local HTML report keep real names — only the persisted artifacts are redacted. The hash is salted per-cluster with the cluster name so the same topic does not get the same digest across distinct clusters.

Telemetry is **opt-in**, **off by default**, and prompted on first run. When enabled, a single ping per `audit` run sends the binary version, OS, the CLI flags used (no values), the cluster size bucket (`1-100`, `100-1k`, `1k-10k`, `10k+` topics), the exit code, and an anonymous run UUID. Topic names, broker addresses, owner data, schema subjects, and source IPs are never sent.

## Comparison

kafka-attic occupies a different layer than the most-cited Kafka tools. The table below points at long-form comparisons for each.

| Tool                                                      | What it does                                                 | Limit                                                          |
|-----------------------------------------------------------|--------------------------------------------------------------|----------------------------------------------------------------|
| [AKHQ](docs/vs/akhq.md) / Provectus Kafka UI              | Manual topic exploration through a web UI                    | No scoring, no automation, no cleanup workflow                 |
| [Cruise Control](docs/vs/cruise-control.md)               | Partition reassignment, broker balance, goal-based moves     | Operates at the broker layer, not the topic-governance layer   |
| [Confluent Health+](docs/vs/confluent-health-plus.md)     | Cluster health for Confluent Platform / Cloud                | Vendor lock; not usable on MSK, Aiven, Redpanda, self-managed  |
| Bespoke scripts                                           | Per-company hand-rolled cleanup notebooks                    | No published methodology, no evidence model, not reproducible  |
| **kafka-attic**                                           | Per-topic ATTIC Score, versioned methodology, read-only CLI  | One-shot — continuous monitoring is out of scope for v1        |

The four comparison docs (`docs/vs/*.md`) cover where each tool overlaps, where it diverges, and whether the two coexist usefully on the same cluster. The short answer in every case is yes: kafka-attic does not replace any of them; it covers a question none of them answer directly.

## FAQ

<details>
<summary><strong>Is it safe to run on production?</strong></summary>

Yes. kafka-attic is read-only by construction. The binary uses `franz-go` configured as an admin / consumer client only; the producer client is statically excluded at compile time and a CI test asserts that no producer-capable code path is reachable. The default scan uses bounded concurrency (configurable via `scan.concurrency`) and backs off on throttling responses, so it is safe to run on busy clusters. The first scan on very large clusters (10k+ topics) is the heaviest because it cold-fetches every offset and group state; subsequent scans benefit from the local cache. For multi-thousand-topic clusters, a `--sample` mode lets you scan a representative subset first to size the impact.
</details>

<details>
<summary><strong>Will kafka-attic delete topics for me?</strong></summary>

No. The cleanup script is intentionally a generated shell artifact that you review and run by hand. kafka-attic never executes a destructive command itself, and the verdict label is `LIKELY_UNUSED`, never `SAFE_DELETE`. Topics with `MISSING_SIGNAL`, `COMPACTED`, `REMOTE_STORAGE`, or `UNKNOWN` evidence on any of Activity / Tenancy / Consumption are excluded from the cleanup script entirely. The script also includes per-topic preflight commands (`kafka-topics --describe` and `kafka-consumer-groups --describe`) so the operator has a final manual check before the delete. The script starts with a warning banner: Kafka has no native dry-run for topic deletion, and every command in the script permanently destroys data.
</details>

<details>
<summary><strong>Does it work on Confluent Cloud / MSK Serverless?</strong></summary>

Yes, with degraded Tonnage. On both Confluent Cloud and MSK Serverless, `DescribeLogDirs` is restricted, so kafka-attic skips the Tonnage sub-signal and redistributes its weight across the remaining four. Confluent Cloud topics on tiered storage / infinite retention also receive the `REMOTE_STORAGE` flag, which caps the verdict at `INSPECT` so no cleanup-script entry is generated for them. The Activity, Tenancy, Intent, and Consumption sub-signals are all collected normally on managed offerings — only the storage-footprint signal degrades.
</details>

<details>
<summary><strong>What Kafka permissions does it need?</strong></summary>

Read-only ACLs are sufficient: `Describe` on `Cluster`, `Topic`, and `Group`; `DescribeConfigs` on `Topic`; `DescribeLogDirs` on `Cluster` (optional — enables Tonnage). For Schema Registry, a read-only API key with `Subject:Read` is enough. For MSK with IAM, the policy needs `kafka:DescribeCluster`, `kafka-cluster:DescribeTopic`, `kafka-cluster:DescribeGroup`, `kafka-cluster:DescribeTopicDynamicConfiguration`, and `kafka-cluster:Connect`. The scan reports which permissions were observed in the `permissions_observed` block of the JSON snapshot, so you can audit access after the fact and see which signals were degraded by missing permissions vs missing data.
</details>

<details>
<summary><strong>How is the score calibrated?</strong></summary>

The weights and curves are published as a versioned methodology in [docs/attic-score-spec-v1.0.md](docs/attic-score-spec-v1.0.md). Defaults (Activity 0.30, Tenancy 0.20, Tonnage 0.10, Intent 0.15, Consumption 0.25) come from an anonymized calibration dataset gathered during the design phase, weighted toward signals that are reliably available on every conformant cluster. All weights, thresholds, and the activity curve are overridable in `kattic.yaml`. The `attic_spec_version` field in every JSON snapshot pins the methodology version used for that scan, so historical snapshots remain reproducible — and comparable via `kattic diff` — even when defaults change in later spec revisions. Two snapshots from different spec versions can be compared at the verdict-band level but not at the numeric-score level; `kattic diff` enforces the version check.
</details>

<details>
<summary><strong>What about seasonal topics that fire monthly?</strong></summary>

Seasonality detection is explicitly out of scope for v1.0. A topic that fires once a month will look stale to a single scan. Two mitigations exist today: (1) the verdict is conservative — topics with records present are capped well below `LIKELY_UNUSED` because Consumption goes to 0 the moment records exist, and (2) the optional history database (`history.enabled: true`) accumulates past scans so the planned v1.1 seasonality detector can pick them up. Until v1.1 ships, treat any monthly / quarterly producer as a topic to add to `exclude_patterns` with `effect: mark_protected`. Protected topics still appear in the report, still get a score, and still surface drift, but the verdict is capped at `ACTIVE` and they are excluded from the cleanup script regardless of score.
</details>

## From one-shot to continuous → Conduktor Console Insights

kafka-attic is one-shot by design. It produces a snapshot you can run from a laptop, a CI job, or a cron container. It does not watch a cluster, it does not page an owner, it does not coordinate cleanups across teams, and it does not keep an audit trail of who deleted what. Those are continuous-monitoring concerns and they require a different runtime — one with a persistent process, RBAC integration, and durable state.

When a team's question moves from *"what does our attic look like?"* to *"alert me when a topic becomes stale, ping the owner, and require an approval before deletion"*, that is the work [Console Insights](https://conduktor.io/products/console?utm_source=kafka-attic&utm_medium=oss&utm_campaign=readme&utm_content=evolution) does. Same evidence model, continuous evaluation, RBAC-aware owner routing, approval workflows, multi-cluster aggregation, and cost attribution / chargeback. kafka-attic stays OSS and stays one-shot; Insights picks up where the one-shot ends.

## Roadmap

**v1.0 (current)**

- Four-subcommand CLI (`scan`, `audit`, `inspect`, `diff`)
- ATTIC Score v1.0.0 — five sub-signals, evidence model, verdict caps, flag taxonomy
- Auth: SASL_PLAIN, SCRAM-SHA-256/512, mTLS, AWS IAM (MSK), OAuth bearer
- Outputs: terminal table, JSON snapshot, CSV, single-file HTML report with cleanup script
- Optional local SQLite history for `kattic diff`
- Tiered storage / `REMOTE_STORAGE` detection on MSK and Confluent Cloud
- Confluent Schema Registry support (`topic_name`, `topic_record` strategies)
- Owner mapping: file / topic config / Backstage / JSON endpoint, with precedence resolver
- Opt-in telemetry; opt-in `audit --share` for anonymized summaries

**v1.1 (planned)**

- Seasonality detection from accumulated history (rolling-rate windows over 7d / 30d / 90d)
- Trend charts in the HTML report
- Terraform `import` block generation for topics to absorb under IaC
- Schema Registry providers beyond Confluent: AWS Glue Schema Registry, Apicurio
- Parquet export of the history database for downstream analytics
- Multi-cluster diff (`kattic diff` across cluster boundaries)

**Companion OSS series** (announced at launch; kafka-attic is chapter 1 under the `attic.conduktor.io` umbrella):

- `kafka-keys` — consumer-group lag forensics: *"why is this group behind?"*
- `kafka-acl-lint` — static analyzer for ACL / RBAC drift across clusters
- `kafka-schema-drift` — diff schemas across environments, flag breaking changes pre-deploy
- `kafka-proxy-probe` — measure PII exposure on topics by inspecting schemas (not data)
- `kafka-tenant-x-ray` — multi-tenancy footprint across a shared cluster

Each companion tool is scoped to a single, well-defined Kafka operational question. None of them replace continuous platform tooling; they are diagnostic CLIs in the spirit of `kubectl`-style point queries.

## Contributing

We use DCO sign-off only — no CLA. Read [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow, build and test commands, the methodology change process, and commit conventions. The project is Apache 2.0 and will remain so; the permanence statement is in CONTRIBUTING.md. Methodology changes (anything that touches `docs/attic-score-spec-*.md`) require a separate review path: any change that alters the formula, default weights, or verdict bands is a `minor` bump at minimum and ships with a migration note.

Bug reports, feature requests, and managed-Kafka compatibility notes are all welcome via GitHub Issues. For platforms not yet listed in the [Managed Kafka support](#managed-kafka-support) matrix, please include the broker version, the auth mode, and a redacted `permissions_observed` block from a JSON snapshot.

## License

Apache License 2.0 — see [LICENSE](LICENSE). The ATTIC Score specification under `docs/attic-score-spec-*.md` is licensed separately under [Creative Commons BY 4.0](https://creativecommons.org/licenses/by/4.0/) so the methodology can be cited, forked, and re-implemented independently of this codebase.

---

Built by [Conduktor](https://conduktor.io?utm_source=kafka-attic&utm_medium=oss&utm_campaign=readme&utm_content=footer). The Kafka authority.
