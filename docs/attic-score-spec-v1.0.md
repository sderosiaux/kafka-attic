# ATTIC Score Specification — v1.0.0

**Status**: Stable
**Spec version**: `1.0.0`
**License**: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) (this specification)
**Reference implementation**: `kafka-attic` v1.0 ([github.com/conduktor/kafka-attic](https://github.com/conduktor/kafka-attic))

---

## Abstract

The ATTIC Score is a vendor-neutral, single-scan heuristic for ranking Apache Kafka topics by how strongly the evidence suggests they are no longer used. It exists because every Kafka platform team eventually accumulates a "topic graveyard" — topics nobody writes to, nobody reads from, nobody owns, and nobody dares delete because nobody can prove it is safe. The score does not prove safety; it ranks topics by disuse evidence so a human can spend review time where it pays off.

ATTIC is an acronym over five sub-signals — **A**ctivity, **T**enancy, **T**onnage, **I**ntent, **C**onsumption — each collected from read-only Kafka admin APIs and an optional Schema Registry. A topic gets one number from 0 to 100 (higher = stronger evidence of disuse), one verdict label from a four-band enum, and a set of structured flags annotating special cases (compacted topics, tiered storage, missing signals, partition skew).

The score is **not a probability**. It is a weighted heuristic over independent sub-scores, capped by an evidence policy that refuses to issue a "likely unused" verdict when the underlying data is incomplete or untrustworthy. Implementations must embed the spec version they conform to (`SpecVersion = "1.0.0"`) in their output so downstream tools can detect methodology drift.

This specification is published independently of any implementation so that other Kafka tooling — Cruise Control extensions, custom observability platforms, internal compliance audits — can adopt the methodology and produce comparable results.

---

## 1. Sub-signals

Each sub-signal returns an integer in `[0, 100]` and an evidence level (`KNOWN`, `ESTIMATED`, `UNKNOWN`). Sub-signals are independent: one is allowed to be ignorant of another.

### 1.1 A — Activity

Days since the most recent record was produced to the topic, as observed by the broker.

Source: `ListOffsets` with `timestamp = -1` (LATEST_TIMESTAMP) per partition, then the maximum across partitions. **No record content is read.**

Formula: piecewise-linear over a configurable curve. The default anchor points are:

| Days since last produce | Sub-score |
|---|---|
| 0   | 0   |
| 30  | 25  |
| 90  | 60  |
| 180 | 80  |
| 365+ | 100 |

Interpolate linearly between anchors. Values past the last anchor are clamped at the anchor's score.

Evidence:

- `KNOWN` — the topic's `message.timestamp.type = LogAppendTime`. Broker-set timestamp; cannot lie.
- `ESTIMATED` — `message.timestamp.type = CreateTime` (the default). Client-supplied timestamp; can lie.
- `UNKNOWN` — broker returns no timestamp (Kafka < 0.10.1, or transient error). Adds `MISSING_SIGNAL`.

### 1.2 T — Tenancy

Health of consumer groups currently targeting the topic. Derived from current state alone; no historical commit timestamps are required. (Reading `__consumer_offsets` to mine commit history is permitted by implementations as an enrichment but is out of scope for the base v1 signal.)

Inputs: `DescribeConsumerGroups` (state + member count) and `ListConsumerGroupOffsets` (committed offsets) joined with `ListOffsets` LATEST per partition (lag).

Rules cascade top-down; the first match wins:

| # | Group situation across all groups consuming this topic | Sub-score |
|---|---|---|
| 1 | At least one group `Stable` with `member_count > 0` | 0 |
| 2 | At least one group `Stable` / `PreparingRebalance` / `CompletingRebalance` (any member count) | 0 |
| 3 | At least one group `Empty` with `committed_offset < latest_offset` | 50 |
| 4 | At least one group `Empty` with `committed_offset == latest_offset` (others `Dead` or `Empty`) | 80 |
| 5 | All groups `Dead` | 100 |
| 6 | No group consumes this topic | 100 |

Evidence: always `KNOWN` when both APIs succeed. `MISSING_SIGNAL` if either is denied.

### 1.3 T — Tonnage

Storage footprint percentile against the rest of the cluster (smaller = higher sub-score).

Formula:

```
tonnage_score = max(0, 100 - p)     where p is the percentile in [0, 100]
```

Smaller topics get a higher sub-score because tiny abandoned topics are exactly the long-tail signal the score is built for.

Evidence:

- `KNOWN` — `DescribeLogDirs` succeeded.
- `ESTIMATED` — bytes inferred from `(latest_offset − earliest_offset)` multiplied by the broker-reported average record size from log segment metadata. **Never** from reading records.
- `UNKNOWN` — neither path available (typical on Confluent Cloud and MSK Serverless).

When Tonnage evidence is `UNKNOWN`, the sub-signal is **skipped**: it contributes nothing to the raw score, and its weight is redistributed proportionally across the remaining sub-signals. Tonnage `UNKNOWN` does **not** add `MISSING_SIGNAL` and does **not** trigger a verdict cap.

### 1.4 I — Intent

Whether a Schema Registry subject targets this topic. Computed only when a Schema Registry is configured.

| Subject strategy | Sub-score = 100 when… | Allowed in v1 |
|---|---|---|
| `topic_name`   | No subject named `<topic>-key` or `<topic>-value` exists | yes |
| `topic_record` | No subject named `<topic>-<recordName>` matches | yes |
| `record_name`  | Cannot be determined from SR alone; sub-signal is **skipped** | skipped |

Evidence: `KNOWN` when SR is reachable and the configured strategy is topic-derivable. When SR is unreachable or the strategy is `record_name`, the sub-signal is **skipped** with weight redistribution. As with Tonnage, a skipped Intent does not add `MISSING_SIGNAL`.

### 1.5 C — Consumption

Whether records currently exist on the topic.

| State | Sub-score |
|---|---|
| `earliest_offset == latest_offset` across all partitions | 100 |
| `earliest_offset > 0` and `earliest_offset == latest_offset` (records existed, retention purged them) | 90 |
| Otherwise (records present) | 0 |

Evidence: `KNOWN` when partition-level `ListOffsets` succeeds; `MISSING_SIGNAL` otherwise.

---

## 2. Weights and raw score

Default weights (must sum to 1.0):

| Letter | Sub-signal | Default weight |
|---|---|---|
| A | Activity | 0.30 |
| T | Tenancy | 0.20 |
| T | Tonnage | 0.10 |
| I | Intent | 0.15 |
| C | Consumption | 0.25 |

The raw score is the weighted sum over the non-skipped sub-signals, with the skipped weights redistributed:

```
let S = set of non-skipped sub-signals
let W = sum of default weights over S
raw_score = sum_{i in S} ( (w_i / W) * score_i )
```

For sub-signals in S whose evidence is `UNKNOWN` (only possible for Activity, Tenancy, Consumption), the sub-score contributes a neutral `50` and the topic gains the `MISSING_SIGNAL` flag.

---

## 3. Evidence model

Two distinct `UNKNOWN` behaviours:

| Sub-signal | UNKNOWN behaviour |
|---|---|
| Tonnage, Intent | **Skipped** — weight redistributed, no `MISSING_SIGNAL` flag, no verdict cap |
| Activity, Tenancy, Consumption | **Missing** — contributes neutral 50, `MISSING_SIGNAL` flag added, verdict capped at `INSPECT` |

This split exists because Tonnage and Intent are routinely unavailable on managed Kafka offerings (log-dirs locked down, no Schema Registry) and treating that as "we don't know" would be misleading. The other three sub-signals come from baseline admin APIs that any conformant Kafka cluster must expose; their absence is genuinely a signal-collection failure that should temper the verdict.

---

## 4. Verdict bands

| Score band | Machine enum | Display label |
|---|---|---|
| ≥ 90 | `LIKELY_UNUSED` | Likely unused |
| 70–89 | `CANDIDATE` | Candidate |
| 40–69 | `INSPECT` | Inspect |
| < 40 | `ACTIVE` | Active |

### 4.1 Verdict caps

The numeric score is computed unchanged; the **verdict label** is capped by these rules. The strictest cap wins. Implementations record the cap reason in `attic.verdict_capped_by`:

| Condition | Cap reason string | Max verdict |
|---|---|---|
| `MISSING_SIGNAL` flag present | `MISSING_SIGNAL` | `INSPECT` |
| Any sub-signal evidence is `ESTIMATED` | `ESTIMATED_EVIDENCE` | `CANDIDATE` |
| `COMPACTED` flag present | `COMPACTED` | `INSPECT` |
| `REMOTE_STORAGE` flag present | `REMOTE_STORAGE` | `INSPECT` |
| `APPEARS_NEVER_USED` flag present without `PURGED` evidence | `APPEARS_NEVER_USED` | `CANDIDATE` |

---

## 5. Flag taxonomy

Flags annotate topics; they never lower the numeric score. Some cap the verdict (see §4.1).

| Machine enum | Meaning |
|---|---|
| `APPEARS_NEVER_USED` | `earliest_offset == latest_offset == 0` across all partitions, no consumer group has any committed offset. Single-scan evidence — cannot prove "never" from one scan |
| `PURGED` | `earliest_offset > 0` and `earliest_offset == latest_offset`. Records existed and retention purged them |
| `OVERSIZED` | Partition count exceeds a configurable threshold while observed message rate is below a configurable floor. **Requires** an external metrics source (Prometheus / JMX). Without metrics, never emitted |
| `SKEWED` | Largest partition is more than `max_ratio_to_average` times the average partition size |
| `ORPHAN_SCHEMA` | Schema Registry has no subject targeting the topic (topic-derived strategies only) |
| `COMPACTED` | `cleanup.policy` includes `compact`. Compacted topics have semantics that change the meaning of "empty" and require human review |
| `REMOTE_STORAGE` | Tiered storage active (MSK Tiered Storage, Confluent Cloud infinite retention, or equivalent). Broker-side bytes do not represent the full topic |
| `MISSING_SIGNAL` | At least one of Activity / Tenancy / Consumption could not be collected |

---

## 6. Worked example

Topic `legacy-events`:

- 287 days since last produce; `message.timestamp.type = LogAppendTime`
- 1 consumer group, state `Dead`
- 12.3 GB storage; sits at percentile 4 of the cluster size distribution
- Schema Registry configured, `topic_name` strategy, no matching subject found
- `earliest_offset > 0` and `latest_offset > earliest_offset` (records present, not purged)

Activity sub-score (piecewise-linear interpolation between 180 → 80 and 365 → 100):

```
80 + (287 − 180) / (365 − 180) * (100 − 80) ≈ 80 + 11.6 ≈ 92
```

Sub-scores and contributions:

```
A =  92  (287 days, interpolated)   weight 0.30  →  27.6
T = 100  (all groups Dead)          weight 0.20  →  20.0
T =  96  (p4, smaller = higher)     weight 0.10  →   9.6
I = 100  (orphan, topic_name)       weight 0.15  →  15.0
C =   0  (records present)          weight 0.25  →   0.0
                                          total =  72.2  →  CANDIDATE
```

Even with strong A/T/T/I evidence, current record presence drags Consumption to 0 and the score lands in `CANDIDATE`, not `LIKELY_UNUSED`. This is the intended conservative behaviour: topics that still hold data require explicit human judgment before any deletion command runs. The `ORPHAN_SCHEMA` flag is set; `verdict_capped_by` is `null` because no cap is needed at this score.

---

## 7. Versioning policy

The ATTIC Score spec follows semver:

- **Patch** (`1.0.x`) — editorial fixes, clarifications, no behaviour change.
- **Minor** (`1.x.0`) — additive only. New flags, new evidence sources, new optional sub-signals. Conforming implementations of `1.0` remain forward-compatible at the verdict-band level.
- **Major** (`2.0.0`) — breaking changes to the formula, default weights, or evidence model. Snapshots produced by `1.x` and `2.x` are not directly comparable.

The version string is embedded in:

- The `attic_spec_version` field of every JSON snapshot.
- The `attic.spec_version` field per topic.
- The `SpecVersion` constant in any reference implementation that re-exports the methodology.

Diff tools comparing two snapshots **must** verify that both share the same `attic_spec_version` before reporting numeric deltas.

---

## 8. Implementations

| Implementation | Status | Notes |
|---|---|---|
| `kafka-attic` v1.0 | reference | Open-source CLI; Go; read-only; the canonical implementation against this spec |

A conforming implementation must:

1. Compute the five sub-signals as defined in §1.
2. Apply weight redistribution for skipped Tonnage / Intent per §2.
3. Apply the evidence model in §3 (skipped vs `MISSING_SIGNAL`).
4. Map raw score to verdict per §4 and apply caps per §4.1.
5. Emit flags from the taxonomy in §5 with the exact machine enum strings.
6. Embed `attic_spec_version = "1.0.0"` (or the version it conforms to) in its output.

Anything beyond these six items — terminal rendering, HTML report, cleanup-script generation, telemetry, history, owner mapping — is implementation-specific and outside the spec.

---

## 9. License

This specification (the document text, the sub-signal formulas, the weights, the band thresholds, the flag taxonomy, the evidence model) is licensed under **[Creative Commons Attribution 4.0 International](https://creativecommons.org/licenses/by/4.0/)**.

You are free to:

- Share — copy and redistribute the material in any medium or format.
- Adapt — remix, transform, and build upon the material for any purpose, including commercially.

Under the following terms:

- **Attribution** — you must give appropriate credit, link to this specification, and indicate if changes were made.

The `kafka-attic` reference implementation is licensed separately under Apache 2.0. The two licenses are intentionally distinct so the methodology can be cited, forked, and re-implemented independently of any single codebase.

---

## Citation

```
ATTIC Score Specification, version 1.0.0.
https://github.com/conduktor/kafka-attic/blob/main/docs/attic-score-spec-v1.0.md
```
