<p align="center">
  <h1 align="center">kafka-attic</h1>
  <p align="center"><em>Find stale, empty, oversized Kafka topics in one read-only scan.</em></p>
</p>

<p align="center">
  <a href="https://github.com/sderosiaux/kafka-attic/releases"><img src="https://img.shields.io/github/v/release/sderosiaux/kafka-attic?display_name=tag&sort=semver&color=blueviolet" alt="Latest release"></a>
  <a href="https://github.com/sderosiaux/kafka-attic/actions/workflows/ci.yml"><img src="https://github.com/sderosiaux/kafka-attic/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License: Apache 2.0"></a>
  <a href="https://goreportcard.com/report/github.com/sderosiaux/kafka-attic"><img src="https://goreportcard.com/badge/github.com/sderosiaux/kafka-attic" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/sderosiaux/kafka-attic"><img src="https://pkg.go.dev/badge/github.com/sderosiaux/kafka-attic.svg" alt="Go Reference"></a>
</p>

<p align="center">
  <a href="docs/attic-score-spec-v1.0.md"><img src="https://img.shields.io/badge/methodology-ATTIC%20Score%E2%84%A2%20v1.0-orange" alt="ATTIC Score spec"></a>
  <a href="https://github.com/sderosiaux/kafka-attic/discussions"><img src="https://img.shields.io/github/discussions/sderosiaux/kafka-attic?color=informational" alt="GitHub Discussions"></a>
  <a href="https://github.com/sderosiaux/kafka-attic/issues"><img src="https://img.shields.io/github/issues/sderosiaux/kafka-attic?color=critical" alt="Open issues"></a>
  <a href="https://conduktor.io?utm_source=kafka-attic&utm_medium=oss&utm_campaign=readme&utm_content=badge"><img src="https://img.shields.io/badge/made%20by-Conduktor-1f6feb" alt="Made by Conduktor"></a>
</p>

---

Every Kafka cluster has a topic graveyard. Nobody dares delete because nobody can prove it's safe. `kafka-attic` scans your cluster, scores every topic against a published methodology, and produces an auditable report you can hand to a topic owner. It is **read-only by construction** — no producer client compiled, no record contents ever fetched, no broker mutation possible.

```text
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

> The first line — `N topics · M likely unused · X TB reclaimable` — is the same number across terminal, JSON, CSV, and HTML outputs. Print it, paste it, share it.

<!-- TODO: replace with asciinema cast once recorded
<p align="center">
  <a href="https://asciinema.org/a/REPLACE_ME" target="_blank">
    <img src="https://asciinema.org/a/REPLACE_ME.svg" alt="kafka-attic demo" width="720"/>
  </a>
</p>
-->

## Try it in 30 seconds

```bash
# install (macOS / Linux)
brew tap sderosiaux/kafka-attic https://github.com/sderosiaux/kafka-attic
brew install kafka-attic

# point at any Kafka cluster reachable over TCP
kattic scan --cluster ./examples/self-managed-sasl-scram.yaml
```

Or without installing anything:

```bash
docker run --rm -v "$PWD:/work" ghcr.io/sderosiaux/kafka-attic:latest \
  scan --cluster /work/kattic.yaml
```

## Table of contents

- [Why](#why)
- [What kafka-attic does](#what-kafka-attic-does)
- [What kafka-attic does not do](#what-kafka-attic-does-not-do)
- [Install](#install)
- [Usage](#usage)
- [How the score works](#how-the-score-works)
- [Managed Kafka support](#managed-kafka-support)
- [Privacy and security](#privacy-and-security)
- [Configuration](#configuration)
- [Limitations](#limitations)
- [Comparison](#comparison)
- [FAQ](#faq)
- [From one-shot to continuous](#from-one-shot-to-continuous)
- [Roadmap](#roadmap)
- [Companion OSS series](#companion-oss-series)
- [Contributing](#contributing)
- [Acknowledgments](#acknowledgments)
- [License](#license)

## Why

Every Kafka platform team faces the same problem:

- Topics outlive the services that produced them. The producer is gone, but the topic still rents disk.
- Onboarding a new team means inheriting a cluster nobody fully owns.
- Storage cost grows linearly with topic count, but the value-per-topic decays.
- Nobody dares delete because deletion is irreversible and there is no shared evidence model.

Existing tools answer adjacent questions — Cruise Control rebalances brokers, AKHQ browses topics, Confluent Health+ watches cluster health — but none of them score topics for cleanup against a published, vendor-neutral methodology. So every team writes their own bash script, with their own ad-hoc thresholds, and the answers are not portable.

`kafka-attic` exists to make topic-cleanup evidence **portable, auditable, and trustworthy** — the same way DORA metrics made deployment health portable across organizations.

## What kafka-attic does

- **Scores every topic** on a 0–100 scale (the ATTIC Score™) over five sub-signals: Activity, Tenancy, Tonnage, Intent, Consumption.
- **Distinguishes evidence levels** — `KNOWN`, `ESTIMATED`, or `UNKNOWN` per sub-signal. Weak evidence caps the verdict so you never delete on hearsay.
- **Flags structural problems** — `OVERSIZED`, `SKEWED`, `ORPHAN_SCHEMA`, `COMPACTED`, `REMOTE_STORAGE`, `APPEARS_NEVER_USED`, `PURGED`, `MISSING_SIGNAL`.
- **Generates a cleanup script** with strict inclusion rules — only `LIKELY_UNUSED` topics with full evidence, never compacted or tiered-storage topics.
- **Diffs snapshots** so you can track reclaim week-over-week (`kattic diff a.json b.json`).
- **Renders four output formats** — terminal table, JSON, CSV, single-file HTML report.
- **Speaks every common auth** — SASL_PLAIN, SCRAM-SHA-256/512, mTLS, AWS IAM (MSK), OAuth bearer.
- **Connects to every common cluster** — self-managed Apache Kafka, MSK Provisioned, MSK Serverless, Confluent Cloud, Aiven, Redpanda.

## What kafka-attic does not do

- **It does not read record contents.** Last-activity timestamps come from `ListOffsets` with `timestamp = -1`, never from a `Fetch`. Keys, values, and headers are never observed, decoded, or stored.
- **It does not mutate the cluster.** The binary statically refuses to compile a producer client. A CI test asserts on every release that no producer-capable code path is reachable.
- **It does not delete topics for you.** The cleanup script is a hand-reviewable shell artifact. Every entry includes a preflight `--describe` pair so the operator has a final check before each delete.
- **It does not require an agent.** Single static binary. No JVM, no librdkafka, no broker-side install. Runs from a laptop, a CI job, or a cron container with TCP access to brokers.
- **It does not phone home by default.** Telemetry is opt-in and off by default. When enabled, the payload contains only the binary version, OS, CLI flag *names* (no values), cluster-size bucket, exit code, and an anonymous run UUID.

## Install

### Homebrew (macOS / Linux)

```bash
brew tap sderosiaux/kafka-attic https://github.com/sderosiaux/kafka-attic
brew install kafka-attic
```

The Homebrew formula lives at [`Formula/kafka-attic.rb`](Formula/), refreshed on every tagged release.

### Scoop (Windows)

```powershell
scoop bucket add kafka-attic https://github.com/sderosiaux/kafka-attic
scoop install kafka-attic
```

The Scoop manifest lives at [`Scoop/kafka-attic.json`](Scoop/), refreshed on every tagged release.

### Docker

```bash
docker pull ghcr.io/sderosiaux/kafka-attic:latest
docker run --rm -v "$PWD:/work" ghcr.io/sderosiaux/kafka-attic:latest \
  scan --cluster /work/kattic.yaml
```

Multi-arch (`linux/amd64`, `linux/arm64`), non-root, distroless base.

### Pre-built binaries

Download from [releases](https://github.com/sderosiaux/kafka-attic/releases) — Linux / macOS / Windows, amd64 / arm64. Every archive ships with a SHA-256 checksum and SLSA-style provenance.

### Go install

```bash
go install github.com/sderosiaux/kafka-attic/cmd/kattic@latest
```

Requires Go 1.22+.

## Usage

```text
kattic scan      Quick read-only scan with terminal output
kattic audit     Full audit with HTML report
kattic inspect   Single-topic deep dive
kattic diff      Compare two prior JSON snapshots
```

Run `kattic <subcommand> --help` for flags.

### Common invocations

```bash
# Quick scan, terminal output (default)
kattic scan --cluster prod.yaml

# JSON snapshot to stdout
kattic scan --cluster prod.yaml --format json > snapshot.json

# Full audit with HTML report
kattic audit --cluster prod.yaml --output report.html

# Single-topic deep dive — every sub-signal, evidence level, raw input
kattic inspect --topic legacy-events --cluster prod.yaml

# Week-over-week reclaim diff
kattic diff scans/2026-05-14.json scans/2026-05-21.json
```

### Reading the output

| Column          | Meaning                                                                                          |
|-----------------|--------------------------------------------------------------------------------------------------|
| `TOPIC`         | Topic name. Replaced with SHA-256 digest in JSON/CSV when `report.redact_topic_names: hash`.     |
| `LAST PRODUCED` | Human time-delta since the most recent record timestamp observed across partitions.              |
| `STORAGE`       | Bytes from `DescribeLogDirs`. ` est` suffix when estimated; `? GB` when truly unknown.           |
| `SCORE`         | ATTIC Score 0–100. Higher = stronger evidence of disuse.                                         |
| `VERDICT`       | `Active` / `Inspect` / `Candidate` / `Likely unused`. Reflects the verdict cap.                  |
| `NOTES`         | Plain-English flags, comma-joined. JSON/CSV keep the machine enums.                              |

## How the score works

Each topic receives an ATTIC Score from 0 to 100. Higher means stronger evidence of disuse. The score is **not a probability** — it is a weighted heuristic over five sub-signals, each independently scored and tagged with an evidence level.

| Sub-signal      | Default weight | What it measures                                                                                |
|-----------------|----------------|--------------------------------------------------------------------------------------------------|
| **A**ctivity    | 0.30           | Days since the most recent record. Piecewise-linear curve over 0/30/90/180/365 days.             |
| **T**enancy     | 0.20           | State of consumer groups targeting the topic. Cascading rules over `Stable` / `Empty` / `Dead`.  |
| **T**onnage     | 0.10           | Storage footprint percentile across the cluster — smaller topics score higher.                   |
| **I**ntent      | 0.15           | Whether a Schema Registry subject targets the topic (Confluent SR in v1.0).                      |
| **C**onsumption | 0.25           | Current record presence — `earliest == latest`, purged by retention, or records present.         |

**Verdict bands**: `LIKELY_UNUSED` (≥ 90), `CANDIDATE` (70–89), `INSPECT` (40–69), `ACTIVE` (< 40).

**Verdict caps** — weak evidence constrains the verdict label without changing the numeric score:
- Any `ESTIMATED` evidence → max `CANDIDATE`
- `MISSING_SIGNAL` on Activity / Tenancy / Consumption → max `INSPECT`
- `COMPACTED` flag → max `INSPECT`
- `REMOTE_STORAGE` flag → max `INSPECT`
- `APPEARS_NEVER_USED` without `PURGED` evidence → max `CANDIDATE`

**The full methodology** — formulas, evidence-level transitions, weight-redistribution math, flag taxonomy, and a worked example — is published as a versioned spec under CC BY 4.0 so other Kafka tooling can adopt it independently of this implementation.

→ Read [`docs/attic-score-spec-v1.0.md`](docs/attic-score-spec-v1.0.md).

## Managed Kafka support

| Cluster                | Tonnage evidence        | Notes                                                                                              |
|------------------------|-------------------------|----------------------------------------------------------------------------------------------------|
| Self-managed Kafka     | `KNOWN`                 | Full `DescribeLogDirs`.                                                                            |
| MSK Provisioned        | `KNOWN`                 | Requires IAM `DescribeClusterV2`.                                                                  |
| MSK Serverless         | `UNKNOWN`               | Log-dir restricted; Tonnage skipped + weight redistributed.                                        |
| Confluent Cloud        | `UNKNOWN`               | Log-dir restricted; `REMOTE_STORAGE` flag emitted on tiered storage or `retention.ms = -1`.        |
| Aiven                  | `KNOWN` or `ESTIMATED`  | Plan-dependent.                                                                                    |
| Redpanda               | `KNOWN`                 | Faithful Kafka admin API.                                                                          |

When Tonnage cannot be measured, its weight is redistributed across the other four sub-signals — the score is still computed, just from one fewer signal, and the JSON snapshot records the degraded evidence for downstream tooling.

## Privacy and security

> kafka-attic does not fetch record contents. Last-activity timestamps come from broker offset-by-timestamp APIs. Keys, values, and headers are never read.

For environments where topic names themselves are sensitive (multi-tenant SaaS, regulated industries), set `report.redact_topic_names: hash` in `kattic.yaml`. Topic, consumer-group, and SR subject names in JSON / CSV / `audit --share` artifacts are replaced with a per-cluster-salted SHA-256 digest. Local terminal output and local HTML report keep real names.

Telemetry is **opt-in** and **off by default**. Prompt on first run. Payload contains binary version, OS, CLI flag names (no values), cluster-size bucket, exit code, and an anonymous run UUID. Topic names, broker addresses, owner data, schema subjects, and source IPs are never sent.

See [`SECURITY.md`](SECURITY.md) for the security disclosure process.

## Configuration

A minimal `kattic.yaml`:

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

Three full reference configs in [`examples/`](examples/):
- [`self-managed-sasl-scram.yaml`](examples/self-managed-sasl-scram.yaml)
- [`msk-iam.yaml`](examples/msk-iam.yaml)
- [`confluent-cloud-oauth.yaml`](examples/confluent-cloud-oauth.yaml)

The full schema is documented in [`SPEC.md`](SPEC.md) Appendix B. Sensitive material is sourced from environment variables (`*_env` keys) so the config file itself is safe to commit.

## Limitations

Honest list of what kafka-attic v1.0 cannot do today:

- **No seasonality detection.** A topic that fires once a month looks stale to a single scan. v1.1 will use the optional history database to detect rolling patterns. Until then, use `exclude_patterns` with `effect: mark_protected` for known monthly/quarterly producers.
- **No continuous monitoring.** kafka-attic is one-shot by design. For "alert me when a topic becomes stale", see [Console Insights](#from-one-shot-to-continuous).
- **No auto-archive workflow.** The cleanup script is hand-reviewable shell, not an automation. By design.
- **Schema Registry: Confluent only in v1.0.** AWS Glue and Apicurio land in v1.1.
- **Tonnage degraded on MSK Serverless and Confluent Cloud.** `DescribeLogDirs` is restricted; the sub-signal is skipped and redistributed across the other four.
- **No multi-cluster aggregation.** Each scan is single-cluster. Cross-cluster diff lands in v1.1.
- **No Terraform `import` generation yet.** Planned for v1.1.

## Comparison

| Tool                                                       | What it does                                                  | Limit                                                          |
|------------------------------------------------------------|---------------------------------------------------------------|----------------------------------------------------------------|
| [AKHQ](docs/vs/akhq.md) / Provectus Kafka UI               | Web UI for manual topic exploration                           | No scoring, no automation, no cleanup workflow                 |
| [Cruise Control](docs/vs/cruise-control.md)                | Broker-level partition rebalancing                            | Different layer — kafka-attic covers topic governance          |
| [Confluent Health+](docs/vs/confluent-health-plus.md)      | Cluster health for Confluent Platform/Cloud                   | Vendor lock — not usable on MSK, Aiven, Redpanda, self-managed |
| Bespoke scripts                                            | Per-company hand-rolled cleanup notebooks                     | No published methodology, no evidence model                    |
| **kafka-attic**                                            | Per-topic ATTIC Score, versioned methodology, read-only CLI   | One-shot — see Console Insights for continuous monitoring      |

Long-form comparisons under [`docs/vs/`](docs/vs/).

## FAQ

<details>
<summary><strong>Is it safe to run on production?</strong></summary>

Yes. kafka-attic is read-only by construction. The binary uses `franz-go` configured as an admin/consumer client only; the producer client is statically excluded at compile time and a CI test asserts that no producer-capable code path is reachable. Scans use bounded concurrency (configurable) and back off on throttling. For multi-thousand-topic clusters, a `--sample` mode lets you size impact first.
</details>

<details>
<summary><strong>Will kafka-attic delete topics for me?</strong></summary>

No. The cleanup script is a generated shell artifact you review and run by hand. kafka-attic never executes a destructive command itself. The verdict is `LIKELY_UNUSED`, never `SAFE_DELETE`. Topics with `MISSING_SIGNAL`, `COMPACTED`, `REMOTE_STORAGE`, or `UNKNOWN` evidence are excluded from the cleanup script. The script ships with per-topic preflight `--describe` commands and a warning banner — Kafka has no native dry-run for topic deletion.
</details>

<details>
<summary><strong>Does it work on Confluent Cloud / MSK Serverless?</strong></summary>

Yes, with degraded Tonnage. `DescribeLogDirs` is restricted on both, so kafka-attic skips the Tonnage sub-signal and redistributes its weight. Confluent Cloud topics on tiered storage or infinite retention also receive `REMOTE_STORAGE`, capped at `INSPECT`. The four other sub-signals collect normally.
</details>

<details>
<summary><strong>What Kafka permissions does it need?</strong></summary>

Read-only ACLs: `Describe` on `Cluster`, `Topic`, and `Group`; `DescribeConfigs` on `Topic`; `DescribeLogDirs` on `Cluster` (optional — enables Tonnage). For Schema Registry, a read-only API key with `Subject:Read`. For MSK IAM: `kafka:DescribeCluster`, `kafka-cluster:DescribeTopic`, `kafka-cluster:DescribeGroup`, `kafka-cluster:DescribeTopicDynamicConfiguration`, `kafka-cluster:Connect`. The scan reports observed permissions so you can audit access after the fact.
</details>

<details>
<summary><strong>How is the score calibrated?</strong></summary>

Defaults (Activity 0.30, Tenancy 0.20, Tonnage 0.10, Intent 0.15, Consumption 0.25) come from an anonymized calibration dataset gathered during the design phase, weighted toward signals reliably available on every conformant cluster. All weights, thresholds, and the activity curve are overridable in `kattic.yaml`. Every JSON snapshot pins the `attic_spec_version` it was computed under, so historical snapshots remain reproducible and comparable via `kattic diff`.
</details>

<details>
<summary><strong>What about seasonal topics that fire monthly?</strong></summary>

Seasonality detection is out of scope for v1.0. Two mitigations exist: (1) the verdict is conservative — Consumption goes to 0 the moment records exist, so topics with current data are capped well below `LIKELY_UNUSED`, and (2) the optional history database (`history.enabled: true`) accumulates past scans so the v1.1 seasonality detector can pick them up. Until then, add monthly/quarterly producers to `exclude_patterns` with `effect: mark_protected`.
</details>

<details>
<summary><strong>Why is the methodology spec under a different license than the code?</strong></summary>

The code is Apache 2.0; the methodology spec (`docs/attic-score-spec-*.md`) is CC BY 4.0. The spec is a piece of documentation meant to be cited, forked, and re-implemented by other Kafka tooling — CC BY 4.0 is the standard license for that kind of artifact. The dual licensing is intentional and reflects how DORA / SPACE / similar measurement frameworks are published.
</details>

## From one-shot to continuous

kafka-attic is one-shot by design. It produces a snapshot you can run from a laptop, a CI job, or a cron container. It does not watch a cluster, it does not page an owner, it does not coordinate cleanups across teams, and it does not keep an audit trail of who deleted what. Those are continuous-monitoring concerns and they require a different runtime — one with a persistent process, RBAC integration, and durable state.

When a team's question moves from *"what does our attic look like?"* to *"alert me when a topic becomes stale, ping the owner, and require an approval before deletion"*, that is the work [**Conduktor Console Insights**](https://conduktor.io/products/console?utm_source=kafka-attic&utm_medium=oss&utm_campaign=readme&utm_content=evolution) does. Same evidence model, continuous evaluation, RBAC-aware owner routing, approval workflows, multi-cluster aggregation, cost attribution.

kafka-attic stays OSS and stays one-shot. Console Insights picks up where the one-shot ends.

## Roadmap

**v1.0** (current)

- Four-subcommand CLI (`scan`, `audit`, `inspect`, `diff`)
- ATTIC Score v1.0.0 — five sub-signals, evidence model, verdict caps, flag taxonomy
- Auth: SASL_PLAIN, SCRAM-SHA-256/512, mTLS, AWS IAM, OAuth
- Outputs: terminal, JSON, CSV, single-file HTML with cleanup script
- Optional local SQLite history for `kattic diff`
- Tiered storage / `REMOTE_STORAGE` detection
- Confluent Schema Registry (`topic_name`, `topic_record`)
- Owner mapping: file / topic config / Backstage / JSON endpoint
- Opt-in telemetry, opt-in `audit --share`

**v1.1** (planned)

- Seasonality detection from accumulated history
- Trend charts in the HTML report
- Terraform `import` block generation
- Schema Registry: AWS Glue, Apicurio
- Parquet export of the history database
- Multi-cluster diff

Open an issue or [Discussion](https://github.com/sderosiaux/kafka-attic/discussions) to influence priorities.

## Companion OSS series

kafka-attic is the first of a planned series of OSS Kafka diagnostic CLIs:

| Tool                     | Question it answers                                                |
|--------------------------|--------------------------------------------------------------------|
| `kafka-attic` *(v1.0)*   | *Which topics are safe to delete?*                                 |
| `kafka-keys`             | *Why is this consumer group behind?*                               |
| `kafka-acl-lint`         | *Do my ACLs drift across clusters?*                                |
| `kafka-schema-drift`     | *Will this schema change break a downstream consumer?*             |
| `kafka-proxy-probe`      | *Which topics expose PII based on schema?*                         |
| `kafka-tenant-x-ray`     | *Who is using how much of this shared cluster?*                    |

Each tool is scoped to a single, well-defined operational question — diagnostic CLIs in the spirit of `kubectl`-style point queries, not platforms.

## Contributing

DCO sign-off only — no CLA. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the workflow, build and test commands, the methodology change process, and commit conventions. The project is Apache 2.0 and will remain so — the permanence statement is in `CONTRIBUTING.md`.

- Bug reports → [Issues](https://github.com/sderosiaux/kafka-attic/issues/new?template=bug_report.yml)
- Feature requests → [Issues](https://github.com/sderosiaux/kafka-attic/issues/new?template=feature_request.yml)
- Questions / discussion → [Discussions](https://github.com/sderosiaux/kafka-attic/discussions)
- Security disclosures → see [`SECURITY.md`](SECURITY.md)
- Code of conduct → [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)

## Acknowledgments

Built on the shoulders of:

- [`franz-go`](https://github.com/twmb/franz-go) — pure-Go Kafka client, the only one with first-class IAM and OAuth support
- [`modernc.org/sqlite`](https://gitlab.com/cznic/sqlite) — pure-Go SQLite (no CGO), powers the local history database
- [`cobra`](https://github.com/spf13/cobra) + [`viper`](https://github.com/spf13/viper) — CLI scaffolding and config loading
- [`testcontainers-go`](https://github.com/testcontainers/testcontainers-go) — Kafka integration tests against real Redpanda containers
- The Kafka community for years of public conversation about topic hygiene that crystallized into this methodology

## License

- **Code** — Apache License 2.0 ([`LICENSE`](LICENSE))
- **Methodology spec** (`docs/attic-score-spec-*.md`) — [Creative Commons BY 4.0](https://creativecommons.org/licenses/by/4.0/), so the methodology can be cited, forked, and re-implemented independently of this codebase

---

<p align="center">
  <a href="https://star-history.com/#sderosiaux/kafka-attic&Date">
    <img src="https://api.star-history.com/svg?repos=sderosiaux/kafka-attic&type=Date" alt="Star History Chart" width="640">
  </a>
</p>

<p align="center">
  Built by <a href="https://conduktor.io?utm_source=kafka-attic&utm_medium=oss&utm_campaign=readme&utm_content=footer">Conduktor</a>. The Kafka authority.
</p>
