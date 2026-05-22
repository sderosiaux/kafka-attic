# kafka-attic — Spec

**Status**: Draft v0.3 · 2026-05-21
**Owner**: Stéphane Derosiaux
**Goal**: Open-source CLI + HTML report that surfaces **stale, empty, oversized, and abandoned Kafka topics** with a per-topic ATTIC Score™. Platform teams use kafka-attic to find their topic graveyard, reclaim storage, and prove what is safe to clean up.

`kafka-attic` is the project and brand. `kattic` is the binary name only — never used as the product name in user-facing surfaces (README, blog, conference talks).

---

## 1. Why now

### Customer signal (Conduktor transcripts, 3 months)

- **Exodus Point** (Aleti, 2026-04-01 09:38): *"We don't know if something is truly unused for a long time"*
- **DCSG** (Poirier, 2026-04-07 19:41): *"stale topics, empty topics, tiny topics… idle days"*
- **Mercedes** (Bischoff, 2026-03-05 26:30): *"it was hard to identify way oversized topics"*
- **Länsförsäkringar** (implicit, governance): no inventory of what's alive

**Universal pain**: every Kafka platform team has a topic graveyard. No one dares delete because no one can prove it's safe.

---

## 2. Scope (v1.0)

### In scope

Connect to any Kafka cluster (SASL_PLAIN, SCRAM, mTLS, IAM, OAuth).

Compute per-topic signals from a **single read-only scan**. No prior history required for any signal to produce a value or `UNKNOWN`.

The **ATTIC Score™** (see §4) combines five sub-signals:

- **A — Activity** — recency of the last produced record
- **T — Tenancy** — presence and state of consumer groups targeting the topic
- **T — Tonnage** — storage footprint relative to the rest of the cluster
- **I — Intent** — whether a Schema Registry subject targets the topic
- **C — Consumption** — current record presence on the topic

Each sub-signal carries an **evidence level**: `KNOWN`, `ESTIMATED`, or `UNKNOWN` (see Appendix E). The overall ATTIC Score is capped by the weakest evidence in any non-skippable signal.

Outputs: terminal table, JSON snapshot, CSV, single-file HTML report. **Read-only** — kafka-attic never mutates the cluster; the binary statically refuses to compile a producer client.

### Out of scope (v1)

- **Seasonality detection** (v1.1)
- **Trend / rolling rates over 7d-30d-90d** (v1.1 — emerges once the optional history DB has accumulated runs)
- **Terraform import block generation** (v1.1)
- **Schema Registry providers other than Confluent** (Glue, Apicurio — v1.1)
- Auto-delete / auto-archive
- Continuous monitoring (Conduktor Console Insights)
- Multi-cluster correlation (v2)

---

## 3. UX

### Commands

```bash
# Quick scan, terminal output
kattic scan --cluster prod.yaml

# Full audit with HTML report
kattic audit --cluster prod.yaml --output report.html

# Single-topic deep dive
kattic inspect --topic legacy-events --cluster prod.yaml

# Compare two prior snapshots (week-over-week reclaim tracking)
kattic diff scans/2026-05-14.json scans/2026-05-21.json

# Share an anonymized summary (opt-in telemetry, see §5)
kattic audit --cluster prod.yaml --share
```

### Terminal output (default)

The terminal uses **human-readable labels**, not the machine enums. Machine enums (`LIKELY_UNUSED`, `ORPHAN_SCHEMA`, etc.) live in JSON/CSV outputs only.

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

The headline line above the table — `N topics · M likely unused · X TB reclaimable` — is the quotable summary every scan prints.

Notes column is plain English. The `--format json` and `--format csv` outputs keep the machine enums.

### HTML report

- Sortable table with the same columns as the terminal, plus the per-signal contribution (A, T, T, I, C) per topic
- Topic detail page with explanation: which signals contributed to the score, with evidence level inline
- Filters: verdict, score range, flag, owner (when owner data is provided), size
- Export: CSV, JSON
- **Reclaim summary**: total TB reclaimable, count by verdict, top 20 candidates by storage
- **Missing signals notice**: which permissions or endpoints were unavailable, and which signals were therefore degraded
- **Cleanup script section** (always included): see §3.1
- **Footer CTA**: a single, factual line linking to Conduktor Console Insights. Not interstitial, not above-the-fold.

### 3.1 Cleanup script section

The cleanup script section is **always present** in the HTML report and on the terminal output of `kattic audit --print-cleanup`. It includes only topics that satisfy **all** of these inclusion rules:

- Verdict is `LIKELY_UNUSED` (≥ 90)
- No `MISSING_SIGNAL` flag
- No `COMPACTED` flag
- No `REMOTE_STORAGE` flag (MSK Tiered Storage, Confluent Cloud infinite retention)
- All five ATTIC sub-signals have evidence `KNOWN` or `ESTIMATED` (no `UNKNOWN`)

For each included topic the section prints:

1. A **warning banner** at the top: *"⚠ Permanent data deletion. Read this section fully. Kafka has no native dry-run for topic deletion. Every command below will permanently destroy data."*
2. A **preflight pair per topic**:
   ```
   kafka-topics    --bootstrap-server $BS --describe --topic legacy-events
   kafka-consumer-groups --bootstrap-server $BS --describe --all-groups --topic legacy-events
   ```
3. The **delete command** for the same topic.
4. The **topic owner** inline when known (sourced per §5.4): `# owner: data-platform@acme.com (Backstage entity: component:default/orders-svc)`.
5. An **inline Conduktor moment**: *"Approval workflow, audit trail, and owner ping before any of these commands run — see Console Insights."* Single sentence. One link. No banner.

Topics that fail any inclusion rule are listed in a separate "Topics omitted from cleanup script" section with the reason.

---

## 4. The ATTIC Score™

A topic's ATTIC Score is a number from 0 to 100 indicating how strongly the evidence suggests the topic is no longer used. Higher score = stronger evidence of disuse.

The score is **not a probability**. It is a weighted heuristic over five sub-signals, each independently scored from 0 to 100. The methodology is published as a versioned spec (this section) and the same version string appears in every JSON snapshot.

### 4.1 Sub-signals and weights

| Letter | Sub-signal | What it measures | Default weight |
|---|---|---|---|
| A | Activity | Days since the most recent record on the topic | 0.30 |
| T | Tenancy | State of consumer groups targeting the topic | 0.20 |
| T | Tonnage | Storage footprint percentile against the cluster | 0.10 |
| I | Intent | Whether any Schema Registry subject targets this topic (when SR is configured) | 0.15 |
| C | Consumption | Current record presence on the topic | 0.25 |

Weights are configurable in `kattic.yaml` and must sum to 1.0.

### 4.2 Sub-signal formulas

Each sub-signal returns a value in [0, 100] **and** an evidence level (`KNOWN`, `ESTIMATED`, `UNKNOWN`).

**Activity (days since last produce, `d`)** — piecewise linear, configurable thresholds:

| d (days) | Sub-score |
|---|---|
| 0 | 0 |
| 30 | 25 |
| 90 | 60 |
| 180 | 80 |
| 365+ | 100 |

Evidence: `KNOWN` if the broker uses `message.timestamp.type = LogAppendTime`. `ESTIMATED` if `CreateTime` (client-controlled timestamp can be arbitrary). `UNKNOWN` if the broker does not return a timestamp via ListOffsets with timestamp = `-1` (rare, very old brokers).

**Tenancy** — derived from `DescribeConsumerGroups` and `ListConsumerGroupOffsets` (no historical commit timestamps required, see §5.2). Rules cascade top-down; the first match wins:

| # | Group situation (across all groups consuming this topic) | Sub-score |
|---|---|---|
| 1 | At least one group in state `Stable` with `member_count > 0` | 0 |
| 2 | At least one group in state `Stable` or `PreparingRebalance` or `CompletingRebalance` (regardless of member count) | 0 |
| 3 | At least one group in state `Empty` with `committed_offset < latest_offset` | 50 |
| 4 | At least one group in state `Empty` with `committed_offset == latest_offset` (others must be `Dead` or `Empty`) | 80 |
| 5 | All groups are in state `Dead` | 100 |
| 6 | No group consumes this topic | 100 |

Evidence: `KNOWN`.

**Tonnage (storage percentile, p ∈ [0, 100])** — smaller topics score higher:

```
tonnage_score = max(0, 100 - p)
```

Evidence: `KNOWN` if `DescribeLogDirs` succeeded. `ESTIMATED` if storage was inferred from `(latest_offset − earliest_offset)` × broker-reported average record size from log metadata (never by reading records). `UNKNOWN` if neither is available — common on Confluent Cloud.

When Tonnage evidence is `UNKNOWN`, the sub-signal is **skipped** (same treatment as Intent), and its weight is redistributed proportionally across the remaining signals. Tonnage `UNKNOWN` does **not** contribute neutral 50 to the raw score, and does **not** add `MISSING_SIGNAL`. The `REMOTE_STORAGE` flag still applies independently when tiered storage is detected, regardless of Tonnage evidence.

**Intent (Schema Registry)** — only computed when Schema Registry is configured:

| Subject strategy | Sub-score = 100 when… | Allowed in v1 |
|---|---|---|
| `topic_name` | No subject named `<topic>-key` or `<topic>-value` exists | ✓ |
| `topic_record` | No subject named `<topic>-<recordName>` matches | ✓ |
| `record_name` | Cannot be determined from SR alone; sub-signal is **skipped** | skipped |

When the sub-signal is skipped (no SR configured, or `record_name` strategy), the Intent weight is **redistributed** proportionally across the other four signals before scoring.

Evidence: `KNOWN` when SR is reachable and strategy is supported; `UNKNOWN` when SR is unreachable (depending on `on_failure: warn`, the sub-signal is skipped with redistribution).

**Consumption (record presence)** — binary with evidence shading:

| State | Sub-score |
|---|---|
| `earliest_offset == latest_offset` across all partitions | 100 |
| `earliest_offset > 0` (records existed but were purged by retention) | 90 |
| Otherwise (records present) | 0 |

Evidence: `KNOWN`.

### 4.3 Verdict bands

| Score | Machine enum | Display label |
|---|---|---|
| ≥ 90 | `LIKELY_UNUSED` | Likely unused |
| 70–89 | `CANDIDATE` | Candidate |
| 40–69 | `INSPECT` | Inspect |
| < 40 | `ACTIVE` | Active |

Thresholds are configurable in `kattic.yaml`.

### 4.4 Evidence policy (verdict caps)

Missing or weak evidence caps the verdict, regardless of computed score:

| Condition | Verdict cap |
|---|---|
| `MISSING_SIGNAL` flag present (Activity / Tenancy / Consumption UNKNOWN) | Max verdict = `INSPECT` |
| Any sub-signal evidence is `ESTIMATED` | Max verdict = `CANDIDATE` |
| `COMPACTED` flag present | Max verdict = `INSPECT` |
| `REMOTE_STORAGE` flag present | Max verdict = `INSPECT` |
| `APPEARS_NEVER_USED` flag present without `PURGED` evidence | Max verdict = `CANDIDATE` |

The numeric score is still printed; the verdict label reflects the cap.

### 4.5 Flags

Flags annotate topics; they never lower the score numerically, but several cap the verdict (§4.4).

| Flag (machine) | Display | Meaning |
|---|---|---|
| `APPEARS_NEVER_USED` | "Appears never used" | Single-scan evidence: `earliest_offset == latest_offset == 0`, no consumer group has committed offsets. Cannot prove "never" from one scan |
| `PURGED` | "Records purged by retention" | `earliest_offset > 0` and `earliest_offset == latest_offset`. Records existed and were deleted by retention |
| `OVERSIZED` | "Over-provisioned partitions" | Partition count exceeds `oversized.max_partitions_for_throughput` and observed rate is below `low_traffic_msgs_per_sec`. **Requires a metrics source** (Prom/JMX). Without metrics, this flag is never emitted |
| `SKEWED` | "Partition load uneven" | Largest partition > `skew.max_ratio_to_average` × average partition size |
| `ORPHAN_SCHEMA` | "No schema reference found" | Schema Registry has no subject targeting this topic (topic-derived strategies only) |
| `COMPACTED` | "Compacted topic; manual review required" | `cleanup.policy` includes `compact` |
| `REMOTE_STORAGE` | "Tiered storage; storage unknown" | MSK Tiered Storage active, Confluent Cloud infinite retention, or any other indicator that data lives off-broker |
| `MISSING_SIGNAL` | "Some signals unavailable" | At least one sub-signal could not be collected |

### 4.6 Worked example

`legacy-events`, 287 days since last produce (LogAppendTime), 0 active consumer groups (one `Dead`), 12.3 GB storage (p4 in cluster size distribution), `topic_name` SR strategy with no matching subject, partition offsets non-zero (records still present, not purged):

Activity sub-score uses piecewise-linear interpolation between 180 days (80) and 365 days (100):
`80 + (287 − 180) / (365 − 180) × (100 − 80) ≈ 80 + 11.6 ≈ 92`

```
A =  92  (287 days, interpolated)  weight 0.30  →  27.6
T = 100  (all groups Dead)         weight 0.20  →  20.0
T =  96  (p4, smaller = higher)    weight 0.10  →   9.6
I = 100  (orphan, topic_name)      weight 0.15  →  15.0
C =   0  (records present)         weight 0.25  →   0.0
                                            total = 72.2  →  CANDIDATE
```

Records present knocks Consumption to 0, dragging the verdict from `LIKELY_UNUSED` down to `CANDIDATE` despite strong A/T/T/I evidence. This is the intended conservative behavior: topics with current data require explicit human judgment before deletion.

---

## 5. How kafka-attic talks to your cluster

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
   │  • Owner mapping             │  optional (file / topic_config / Backstage / JSON endpoint)
   │  • Tiered storage probes     │  per cluster type, see §5.5
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

- **Language**: Go (single static binary; brew, scoop, docker, raw GitHub release). **Kafka client**: `franz-go` (pure Go, supports SASL/IAM/OAuth without librdkafka).
- **No producer client**: `kafka-attic` does not import any producer-capable code path. This is enforced by linter + CI test.
- **History**: optional local SQLite (`~/.kattic/history.db`). Used only for trend charts in the HTML report and for `kattic diff`. **Scoring is fully determined by the current scan** and never depends on history.
- **No agent**: pure pull. Runs anywhere with broker connectivity.

### 5.1 Last-activity collection

`kafka-attic` reads last-activity via `ListOffsets` (KIP-79, Kafka 0.10.1+) with `timestamp = -1` (LATEST_TIMESTAMP variant per partition), then takes the maximum across partitions.

- If the topic's `message.timestamp.type = LogAppendTime`: evidence `KNOWN`.
- If `CreateTime` (default): evidence `ESTIMATED`. Client-supplied timestamps can lie; the report explains this caveat per topic.
- If the broker returns no timestamp (Kafka < 0.10.1, or a broker-side error): evidence `UNKNOWN` and the topic is flagged `MISSING_SIGNAL`.

**No record content is read.** `ListOffsets` returns a timestamp, not a record.

### 5.2 Consumer abandonment without commit history

The original spec required "last commit time per group" — that information is **not** available via standard admin APIs without consuming `__consumer_offsets`. v1 avoids that and instead derives Tenancy from current state only:

- `DescribeConsumerGroups` → group state (`Stable`, `Empty`, `Dead`, `PreparingRebalance`, …) and current member count
- `ListConsumerGroupOffsets` → currently committed offsets per topic-partition
- Combined with `ListOffsets` LATEST per partition → lag

The sub-signal formulas in §4.2 use only these. v1.1 may add commit-history reading from `__consumer_offsets` for richer signal.

### 5.3 Required Kafka permissions

Minimum:

- `Describe` on cluster
- `Describe` on each topic in scope
- `Describe` on consumer groups

Optional (richer signals):

- `DescribeLogDirs` — exact `Tonnage`. Without it, `Tonnage` may be `ESTIMATED` or `UNKNOWN`
- Confluent Schema Registry read on subjects — required for `Intent`
- Topic configs `read` — required for `COMPACTED` flag and tiered-storage detection

`kafka-attic` detects each missing permission per signal and continues with what it has. The HTML report and the JSON `permissions_observed` block both spell out what was missing.

### 5.4 Owner mapping sources

`owners:` in `kattic.yaml` accepts one of four sources for v1:

| Source | How it works |
|---|---|
| `file` | YAML file with `pattern → owner` entries. First-match wins. Patterns are regex on topic name |
| `topic_config` | Pull a configured topic config key (e.g. `owner`) from `DescribeConfigs` for each topic |
| `backstage` | Hit a Backstage Catalog API: `GET /api/catalog/entities/by-name/{kind}/{namespace}/{name}`. Resolve via a configurable pattern from topic name → entity ref. Fallback to `relations.ownedBy` |
| `json` | Generic HTTP `GET <url>?topic=<name>` returning `{ "owner": "...", "team": "..." }`. Headers, auth, and `jq`-style extraction path configurable |

`owners.precedence`: array of source names in priority order (first non-null wins). Default owner = `null`. Invalid patterns log a warning and are skipped.

### 5.5 Managed Kafka and tiered storage

`kafka-attic` detects cluster type and tiered storage as follows:

| Indicator | Cluster type / flag |
|---|---|
| Topic config `remote.storage.enable=true` | MSK Tiered Storage → `REMOTE_STORAGE` flag |
| Topic config `confluent.placement.constraints` or `confluent.tier.enable` | Confluent Cloud / Tiered Storage → `REMOTE_STORAGE` flag |
| `retention.ms = -1` AND broker is Confluent Cloud | Infinite retention → `REMOTE_STORAGE` flag |
| `DescribeLogDirs` returns `LEADER_NOT_AVAILABLE` or unauthorized | Managed cluster restricting log-dir → Tonnage `UNKNOWN` |

`REMOTE_STORAGE` topics are **always** excluded from the cleanup script (§3.1) and capped at `INSPECT` verdict (§4.4). Tonnage is shown as `?` in the terminal and `null` in JSON when truly unknown.

### 5.6 Privacy

- `kafka-attic` does not fetch record contents. Last-activity uses `ListOffsets`, not `Fetch`.
- No keys, values, or headers are read at any point.
- The managed-Kafka Tonnage estimate uses broker-reported average record size from log segment metadata (`DescribeLogDirs.LogDirInfo.size / sum(record_count)`). When that is restricted, Tonnage is `UNKNOWN`. **There is no record sampling.**
- Topic names, consumer group names, and Schema Registry subject names are stored in the snapshot. They can be sensitive (often encode customer IDs, jurisdictions). Set `report.redact_topic_names: hash` in `kattic.yaml` to SHA-256 names in JSON / CSV / shared artifacts; the local HTML report can still show plaintext. Default is `none`.

### 5.7 Telemetry (opt-in)

On first run, `kafka-attic` prompts for opt-in anonymous telemetry. Default: **off**. Stored consent in `~/.kattic/config.json`.

When enabled, each `audit` run sends a single ping containing:

- kafka-attic version, OS, CLI flags used (no values)
- Cluster size bucket: `1-100`, `100-1k`, `1k-10k`, `10k+` topics
- Exit code
- Anonymous run UUID

Never sent: topic names, broker addresses, owner data, schema subjects, IP addresses (the receiving endpoint discards source IP at ingress).

`kattic audit --share` is a separate explicit action: it uploads the **anonymized summary** (per-verdict counts, reclaimable bytes bucketed, no topic names) and returns a shareable URL at `attic.conduktor.io/r/<id>`. Each share is attributable; opt-in per invocation.

---

## 6. Evolution path → Conduktor Console Insights

The CTA appears in exactly two places: **inline in the cleanup script section** (§3.1) where intent is highest, and as a single-line footer on the HTML report. The spec's §1 Goal contains no funnel language.

| Capability | kafka-attic (OSS) | Conduktor Console Insights |
|---|---|---|
| One-shot scan | ✅ | ✅ |
| HTML report | ✅ | ✅ (real-time dashboard) |
| Continuous monitoring | ❌ | ✅ |
| Multi-cluster aggregation | ❌ | ✅ |
| Owner-aware (RBAC integration) | file / config / Backstage / JSON | ✅ "ping the owner" |
| Approval workflow before deletion | ❌ | ✅ |
| Audit trail of cleanups | ❌ | ✅ |
| Cost attribution / chargeback | ❌ | ✅ |
| Alerts ("topic became stale") | ❌ | ✅ |
| Historical trend (> 1 yr) | local SQLite only | ✅ centralized |
| Integration with App Catalog | ❌ | ✅ |

---

## 7. Differentiation vs existing tools

| Tool | Coverage | Limits |
|---|---|---|
| Cruise Control | Partition reassignment, anomaly detection, goal-based optimization | Different problem (broker balance, not topic cleanup) |
| Confluent Health+ | Cluster health on Confluent Cloud/Platform | Vendor lock |
| AKHQ / Provectus UI | Manual topic exploration | No scoring, no automation, no cleanup workflow |
| Bespoke scripts | Per-company hand-rolled | Reinventing the wheel, no published methodology |
| **kafka-attic** | **Vendor-neutral, ATTIC Score™, single binary, read-only, cleanup-aware** | New |

---

## 8. Companion OSS series (announced publicly)

`kafka-attic` is the first of a planned OSS series under the `attic.conduktor.io` umbrella. The series is announced at launch so kafka-attic reads as chapter 1, not as a one-shot:

| Tool | One-line pitch | Status |
|---|---|---|
| `kafka-attic` | Find your topic graveyard | v1.0 — this spec |
| `kafka-keys` | Consumer-group lag forensics — *"why is this group behind?"* | planned |
| `kafka-acl-lint` | Static analyzer for ACLs / RBAC drift across clusters | planned |
| `kafka-schema-drift` | Diff schemas across environments, flag breaking changes pre-deploy | planned |
| `kafka-proxy-probe` | Measure PII exposure on topics by inspecting schemas (not data) | planned |

The repo's README and the conduktor.io landing page both link to this roadmap.

---

## 9. Milestones

1. **M0 — Scaffold**: Go module, cobra CLI, franz-go connection (SASL_PLAIN + SCRAM + mTLS + IAM + OAuth)
2. **M1 — Collector**: metadata, `ListOffsets` timestamp probes, consumer group state, optional Confluent SR client
3. **M2 — ATTIC scorer**: A/T/T/I/C sub-signals, verdict + verdict caps, flag taxonomy
4. **M3 — Owner sources**: file + topic_config + Backstage + JSON endpoint, precedence resolver
5. **M4 — Renderer**: terminal output (human labels), JSON, CSV
6. **M5 — HTML report**: single-file HTML with per-signal contribution view, cleanup script section with inclusion rules, missing-signals notice
7. **M6 — Managed Kafka mode**: tiered storage detection (MSK + Confluent), Tonnage degradation, `REMOTE_STORAGE` flag
8. **M7 — Telemetry**: opt-in first-run prompt, `audit --share`, UTM-tagged CTA links
9. **M8 — History (optional)**: local SQLite for trend charts and `kattic diff`
10. **M9 — Methodology spec + dataset**: publish the ATTIC Score™ spec (this section) at a stable URL, ship anonymized calibration dataset
11. **M10 — Public beta**: GitHub release, brew tap (`conduktor/tap/kafka-attic`), Reddit drop, blog post, asciinema, `awesome-kafka` PR
12. **M11 — v1.1**: seasonality, Terraform import blocks, multi-cluster diff, Glue + Apicurio SR, rolling-rate windows once history accumulates

---

## 10. Open questions (resolved unless noted)

- **History storage**: SQLite local for v1; Parquet export in v1.1
- **Auth coverage at launch**: SASL_PLAIN + SCRAM + mTLS + IAM + OAuth — all in v1.0
- **License**: Apache 2.0. **Governance**: DCO-only, no CLA. CONTRIBUTING.md ships at launch with a permanence statement
- **Naming**: `kafka-attic` everywhere user-facing; `kattic` is the binary name only
- **Distribution**: brew (`conduktor/tap/kafka-attic`), scoop, docker, GitHub release binaries
- **Kafka client library**: `franz-go` (pure Go, no librdkafka)

---

## 11. Risks

- **False positives** (deleting a genuinely-seasonal topic that just hasn't fired this window) → seasonality is explicitly out of scope for v1, conservative weights, `INSPECT` verdict for ambiguous, `COMPACTED` and `REMOTE_STORAGE` topics protected from `LIKELY_UNUSED`, README states the limitation loudly
- **Privacy / sensitive topics** → no record content read; only metadata, offsets, group state. Topic-name redaction available
- **Performance on huge clusters (10k+ topics)** → parallel collection with configurable concurrency, backoff on managed-Kafka throttling (especially MSK), sampling mode for first scan, resumable from cache
- **Cleanup script misuse** → inclusion rules are strict (§3.1), warning banner, preflight commands per topic, no LIKELY_UNUSED label without full evidence, no commands for COMPACTED / REMOTE_STORAGE / MISSING_SIGNAL
- **Score overclaim** → verdict label is `LIKELY UNUSED`, never `SAFE DELETE`. The methodology is published as a versioned spec
- **Vendor-OSS perception** → CONTRIBUTING.md publishes governance + Apache permanence statement; CTA is inline at the cleanup script (intent moment) and a single line in the footer, not interstitial

---

## 12. Success metrics (12 months)

- ≥ 1,000 GitHub stars
- ≥ 50 documented cleanup operations (community testimonials)
- ≥ 5 Conduktor demos attributed to kafka-attic funnel (attribution via UTM on report CTA links + `audit --share` shareable URLs)
- ≥ 1,000 `audit --share` invocations (telemetry signal of real-world usage)
- Featured in `awesome-kafka`, `awesome-go`, `awesome-sre`, Console.dev
- First *State of Kafka Topic Hygiene* report published with ≥ 100 opted-in clusters of aggregated data

---

## Appendix A — HTML report sections

1. **Executive summary** — topics scanned, count by verdict, **reclaimable TB** as the hero number
2. **Verdict breakdown** — pie chart by verdict
3. **Top candidates** — table sorted by score
4. **Per-signal contribution** — for any topic, click to see how A/T/T/I/C contributed and the evidence level
5. **Flag highlights** — grouped views for `OVERSIZED`, `SKEWED`, `ORPHAN_SCHEMA`, `APPEARS_NEVER_USED`, `PURGED`, `COMPACTED`, `REMOTE_STORAGE`
6. **Missing signals** — which permissions or endpoints were unavailable, which topics are therefore capped
7. **Cleanup script** (§3.1) — inclusion rules stated, warning banner, preflight + delete per topic, owner inline, inline Conduktor moment
8. **Topics omitted from cleanup script** — with reason
9. **Footer** — single-line factual link to Conduktor Console Insights

---

## Appendix B — Config file example

```yaml
# kattic.yaml
cluster:
  name: prod-msk
  bootstrap: b-1.msk.eu-west-1.amazonaws.com:9098
  auth:
    type: iam              # sasl_plain | scram | mtls | iam | oauth
    region: eu-west-1
    # IAM details (defaults match AWS SDK behavior):
    profile: null          # null → use AWS_PROFILE / default chain
    assume_role_arn: null
    web_identity: null     # for EKS IRSA

schema_registry:           # optional — Confluent SR only in v1
  provider: confluent
  url: https://psrc-xyz.eu-west-1.aws.confluent.cloud
  auth:
    type: basic            # basic | bearer | none
    username_env: SR_USER
    password_env: SR_PASS
  subject_strategy: topic_name   # topic_name | topic_record | record_name (record_name → Intent skipped)
  on_failure: warn         # warn | fail

owners:                    # optional — populates the 'owner' filter and cleanup script annotations
  precedence: [backstage, file, topic_config, json]
  file:
    path: ./owners.yaml    # entries: { pattern: '^orders-.*', owner: 'team-orders@acme.com' }
  topic_config:
    key: owner             # topic config key holding the owner
  backstage:
    url: https://backstage.acme.com
    auth:
      type: bearer
      token_env: BACKSTAGE_TOKEN
    entity_pattern: 'component:default/{topic}-svc'
    fallback_relation: ownedBy
  json:
    url: 'https://owners.acme.com/lookup?topic={topic}'
    headers:
      Authorization: 'Bearer ${OWNERS_TOKEN}'
    extract: '.team'       # jq-style path

attic_score:
  spec_version: 1.0.0      # frozen reference to the methodology version
  weights:
    activity: 0.30
    tenancy: 0.20
    tonnage: 0.10
    intent: 0.15
    consumption: 0.25
  thresholds:
    likely_unused: 90
    candidate: 70
    inspect: 40
  activity_curve:          # piecewise linear (days → sub-score)
    - { days: 0,   score: 0 }
    - { days: 30,  score: 25 }
    - { days: 90,  score: 60 }
    - { days: 180, score: 80 }
    - { days: 365, score: 100 }

oversized:
  max_partitions_for_throughput: 12
  low_traffic_msgs_per_sec: 1
  requires_metrics: true   # OVERSIZED is never emitted without a metrics source

skew:
  max_ratio_to_average: 4

metrics:                   # optional enrichment, enables OVERSIZED + rate trends
  source: prometheus       # prometheus | jmx
  prometheus:
    url: https://prom.acme.com
    query_msgs_per_sec: 'sum by (topic) (rate(kafka_topic_partition_current_offset[5m]))'

history:
  enabled: true
  path: ~/.kattic/history.db
  retention_days: 730
  # history powers trend charts and `kattic diff` only.
  # ATTIC scoring is determined from a single scan and never depends on history.

exclude_patterns:
  defaults: true           # apply the built-in default exclusions below
  effect: omit_from_score  # omit_from_score | mark_protected
  additional:
    - '^my-internal-.*'
  # Defaults (always applied when defaults: true):
  #   ^__.*                 internal topics (offsets, txn state, ...)
  #   ^_schemas$            schema registry storage
  #   .*\.dlq$              DLQs
  #   .*-changelog$         Kafka Streams changelogs
  #   .*-repartition$       Kafka Streams repartition topics
  #   ^mm2-.*               MirrorMaker 2 internals
  #   .*\.replica$          replication targets

protected_cleanup_policies:
  - compact                # COMPACTED topics never reach LIKELY_UNUSED

telemetry:
  enabled: false           # opt-in; first-run prompt may flip to true
  endpoint: https://telemetry.conduktor.io/attic
  include_anonymous_run_uuid: true

report:
  format: html
  output: ./attic-report.html
  include_cleanup_script: true
  redact_topic_names: none    # none | hash → SHA-256 names in JSON/CSV/shared artifacts
  utm:
    source: kafka-attic
    medium: oss
    campaign: report
```

### `exclude_patterns.effect` semantics

| Value | Counts | JSON snapshot | `kattic diff` | Cleanup script |
|---|---|---|---|---|
| `omit_from_score` | excluded | excluded | excluded | excluded |
| `mark_protected` | shown, scored, verdict capped at `ACTIVE` | included with `excluded_by_pattern: true` | included | excluded |

Default: `omit_from_score`.

---

## Appendix C — Snapshot / JSON output schema

The JSON output is the canonical snapshot format consumed by `kattic diff`. Schema version follows semver; minor versions are additive only.

```json
{
  "schema_version": "1.0.0",
  "attic_spec_version": "1.0.0",
  "generated_at": "2026-05-21T09:38:00Z",
  "kafka_attic_version": "1.0.0",
  "cluster": {
    "name": "prod-msk",
    "bootstrap": "b-1.msk.eu-west-1.amazonaws.com:9098",
    "detected_type": "msk",
    "kafka_version_reported": "3.7.0"
  },
  "scan": {
    "topic_count_scanned": 4821,
    "topic_count_excluded_by_pattern": 142,
    "duration_ms": 18420,
    "permissions_observed": {
      "describe_cluster": true,
      "describe_topics": true,
      "describe_configs": true,
      "describe_groups": true,
      "describe_log_dirs": true,
      "schema_registry_read": true
    },
    "missing_signals_global": [],
    "config_snapshot": {
      "attic_weights": { "activity": 0.30, "tenancy": 0.20, "tonnage": 0.10, "intent": 0.15, "consumption": 0.25 },
      "thresholds": { "likely_unused": 90, "candidate": 70, "inspect": 40 },
      "activity_curve": [{"days":0,"score":0},{"days":30,"score":25},{"days":90,"score":60},{"days":180,"score":80},{"days":365,"score":100}]
    }
  },
  "topics": [
    {
      "name": "legacy-events",
      "name_redacted": null,
      "partitions": 6,
      "replication_factor": 3,
      "cleanup_policy": "delete",
      "retention_ms": 604800000,
      "remote_storage_enabled": false,
      "message_timestamp_type": "LogAppendTime",
      "last_produce_ts": "2025-08-08T11:02:14Z",
      "earliest_offset_sum": 145203,
      "latest_offset_sum": 12847291,
      "storage": {
        "bytes": 13207180800,
        "source": "log_dir",
        "evidence": "KNOWN"
      },
      "partition_metrics": [
        {"partition": 0, "earliest_offset": 24180, "latest_offset": 2141215, "size_bytes": 2201196800, "leader": 1},
        {"partition": 1, "earliest_offset": 24216, "latest_offset": 2141050, "size_bytes": 2200582400, "leader": 2}
      ],
      "consumer_groups": [
        {"group_id": "ingest-v1", "state": "Dead", "member_count": 0, "committed_offset_sum": 12847291, "lag_sum": 0}
      ],
      "schema_registry": {
        "subject_strategy": "topic_name",
        "subjects_found": [],
        "evidence": "KNOWN"
      },
      "attic": {
        "spec_version": "1.0.0",
        "sub_scores": {
          "activity":    { "score":  92, "evidence": "KNOWN", "input": {"days_since_last_produce": 287} },
          "tenancy":     { "score": 100, "evidence": "KNOWN", "input": {"all_groups_dead": true} },
          "tonnage":     { "score":  96, "evidence": "KNOWN", "input": {"percentile": 4} },
          "intent":      { "score": 100, "evidence": "KNOWN", "input": {"orphan": true} },
          "consumption": { "score":   0, "evidence": "KNOWN", "input": {"earliest_eq_latest": false} }
        },
        "raw_score": 72.2,
        "verdict": "CANDIDATE",
        "verdict_capped_by": null
      },
      "flags": ["ORPHAN_SCHEMA"],
      "owner": {
        "value": "data-platform@acme.com",
        "source": "backstage",
        "entity_ref": "component:default/orders-svc"
      },
      "signals_missing": []
    }
  ],
  "telemetry": {
    "anonymous_run_uuid": "d4a1b3c2-9f1e-4b80-9b3f-2cf2a1b1f0e9",
    "shared_summary_url": null
  }
}
```

`storage.source` is one of: `log_dir` (exact), `estimate` (offsets × broker-reported average record size from log segment metadata), or `unknown`.
`signals_missing` lists per-topic signals that could not be collected; non-empty value adds `MISSING_SIGNAL` to flags.
`verdict_capped_by`, when non-null, names the cap rule that lowered the verdict from the raw score band (e.g., `"ESTIMATED_EVIDENCE"`, `"COMPACTED"`, `"REMOTE_STORAGE"`).

---

## Appendix D — Verdict and flag reference

### Verdicts

| Machine enum | Display label | Score band | Meaning |
|---|---|---|---|
| `LIKELY_UNUSED` | Likely unused | ≥ 90 | Strong single-scan evidence of disuse; eligible for the cleanup script |
| `CANDIDATE` | Candidate | 70–89 | Strong signals but at least one caveat (estimated evidence, never-used flag, etc.) |
| `INSPECT` | Inspect | 40–69 | Mixed signals or protective flag (compacted, remote storage, missing signal) |
| `ACTIVE` | Active | < 40 | Currently used or protected |

### Flags

| Machine enum | Display label | Caps verdict at |
|---|---|---|
| `APPEARS_NEVER_USED` | "Appears never used (low evidence)" | `CANDIDATE` |
| `PURGED` | "Records purged by retention" | — |
| `OVERSIZED` | "Over-provisioned partitions" | — |
| `SKEWED` | "Partition load uneven" | — |
| `ORPHAN_SCHEMA` | "No schema reference found" | — |
| `COMPACTED` | "Compacted topic; manual review required" | `INSPECT` |
| `REMOTE_STORAGE` | "Tiered storage; storage unknown" | `INSPECT` |
| `MISSING_SIGNAL` | "Some signals unavailable" | depends on which signal (see Appendix E) |

---

## Appendix E — Signal evidence model

For each sub-signal, evidence depends on cluster type and configuration. The table below is normative.

| Sub-signal | KNOWN when | ESTIMATED when | UNKNOWN when |
|---|---|---|---|
| Activity | `message.timestamp.type = LogAppendTime` and `ListOffsets` returns timestamp | `message.timestamp.type = CreateTime` and `ListOffsets` returns timestamp | broker < 0.10.1 or `ListOffsets` error → `MISSING_SIGNAL` |
| Tenancy | `DescribeConsumerGroups` + `ListConsumerGroupOffsets` both succeed | — (always `KNOWN` or `UNKNOWN`) | either API denied → `MISSING_SIGNAL` |
| Tonnage | `DescribeLogDirs` returns sizes | log segment metadata returns size + record count, broker computes average locally | both unavailable (typical Confluent Cloud) → Tonnage **skipped**, weight redistributed (no `MISSING_SIGNAL`) |
| Intent | SR reachable, strategy is `topic_name` or `topic_record` | — | SR unreachable (with `on_failure: warn`) or strategy is `record_name` → Intent **skipped**, weight redistributed |
| Consumption | `ListOffsets` per partition succeeds | — | partition-level `ListOffsets` error → `MISSING_SIGNAL` |

Two distinct UNKNOWN behaviours:

- **Skipped (Tonnage, Intent only)** — the sub-signal contributes nothing; its weight is redistributed proportionally across the remaining sub-signals before scoring. No `MISSING_SIGNAL` flag is added solely on this basis. Verdict caps from §4.4 do **not** trigger.
- **`MISSING_SIGNAL` (Activity, Tenancy, Consumption)** — the sub-signal contributes a neutral 50 to the raw score, the topic gains the `MISSING_SIGNAL` flag, and verdict caps from §4.4 apply (max `INSPECT`).

Only Activity, Tenancy, and Consumption can be `MISSING_SIGNAL` in v1. Tonnage and Intent are always either present or skipped.

Cluster-type defaults observed in practice:

| Cluster type | Tonnage default evidence | Notes |
|---|---|---|
| Self-managed Kafka | `KNOWN` | full `DescribeLogDirs` |
| MSK provisioned | `KNOWN` (with IAM `DescribeClusterV2`) | log-dir works |
| MSK Serverless | `UNKNOWN` | log-dir restricted |
| Confluent Cloud | `UNKNOWN` | log-dir restricted; `REMOTE_STORAGE` for tiered storage |
| Aiven | `KNOWN` or `ESTIMATED` | varies by plan |
| Redpanda | `KNOWN` | full log-dir support |
