# ATTIC Score Calibration Dataset

`synthetic-clusters.json` is a small set of **synthetic-but-realistic** snapshot summaries used to calibrate, regression-test, and document the ATTIC Score methodology. It is the seed of the future *State of Kafka Topic Hygiene* report once opted-in, real-world data accumulates via `kattic audit --share`.

## What this is

- Distribution summaries for three synthetic clusters across the realistic size spectrum: **small** (~50 topics), **medium** (~500 topics), **large** (~5,000 topics).
- Per-cluster verdict counts, score histogram, flag counts, mean and median score.
- Cluster-type and tonnage-evidence variety: a self-managed Kafka, an MSK-provisioned cluster with full log-dir access, and a Confluent Cloud cluster with restricted log-dirs (Tonnage `UNKNOWN`, weight redistributed).
- Aggregated totals across all clusters.

## What this is not

- **Not real customer data.** Every number was produced by a deterministic generator (`tools/gen-calibration`). No real cluster contributed.
- **Not per-topic rows.** Distribution counts only. The file is intentionally small enough to skim and diff.
- **Not authoritative.** These numbers are illustrative. They show what the ATTIC Score is *capable* of distinguishing, not what any specific organisation will see.

## How it was generated

```bash
# Default seed (idempotent — same seed always yields the same JSON):
go run ./tools/gen-calibration > testdata/calibration/synthetic-clusters.json

# Different seed for an alternative fixture (do not commit unless intentional):
go run ./tools/gen-calibration --seed 7 > /tmp/alt.json
```

The generator lives at `tools/gen-calibration/main.go`. It uses the constants from `pkg/atticspec` so the dataset and the spec stay in lockstep. The seed and the spec version are embedded in the output.

## How to read it

- `verdicts` — count of topics in each verdict band.
- `score_histogram` — 10-wide buckets from 0 to 100, useful for sanity-checking weight or threshold tweaks against the dataset.
- `flag_counts` — independent draws, so totals may sum to more than `topic_count` (a topic can carry multiple flags).
- `reclaimable_bytes` — sum of `LIKELY_UNUSED` topic sizes, excluding `COMPACTED`, `REMOTE_STORAGE`, and `MISSING_SIGNAL` topics. This mirrors the inclusion rules for the cleanup-script section.
- `tonnage_default_evidence` — the expected Tonnage evidence for that cluster type (`KNOWN` for self-managed and MSK provisioned, `UNKNOWN` for Confluent Cloud / MSK Serverless where log-dirs are locked down). See spec section 3.

## Regenerating

The file is committed; do not regenerate casually. If a methodology change requires new calibration numbers:

1. Bump constants in `pkg/atticspec/spec.go`.
2. Update the spec version test in `pkg/atticspec/spec_test.go`.
3. Re-run the generator with the **same seed** (`--seed 42`) so the only diff is the methodology change.
4. Land both the spec change and the regenerated calibration file in one commit so they are reviewable together.

## Provenance

- Spec version embedded: see `attic_spec_version` field.
- Generator: `tools/gen-calibration`.
- License: same as the spec — CC BY 4.0 for the data, Apache 2.0 for the generator code.
