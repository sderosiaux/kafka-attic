// Package atticspec is the public, importable reference for the ATTIC Score(TM)
// methodology used by kafka-attic.
//
// This package is intentionally a thin, dependency-free re-export of the
// methodology constants so third-party tools can implement against the same
// version-pinned spec without depending on the kafka-attic CLI internals.
//
// # Score formula
//
// For each topic, five sub-signals are scored independently in [0, 100]:
//
//	A - Activity     days since last produced record
//	T - Tenancy      state of consumer groups targeting the topic
//	T - Tonnage      storage footprint percentile against the cluster
//	I - Intent       whether a Schema Registry subject targets the topic
//	C - Consumption  current record presence on the topic
//
// The raw ATTIC score is the weighted sum:
//
//	raw = sum( w_i * score_i )  over the non-skipped sub-signals
//
// Tonnage and Intent may be "skipped" when their evidence is UNKNOWN; their
// weight is redistributed proportionally across the remaining sub-signals
// before scoring. Activity, Tenancy, and Consumption can never be skipped;
// when their evidence is UNKNOWN they contribute a neutral 50 to the raw
// score and the topic gains the MISSING_SIGNAL flag.
//
// # Evidence levels
//
// Each sub-signal carries one of:
//
//	KNOWN      - measured directly from the broker / SR
//	ESTIMATED  - inferred from secondary metadata; client-controllable
//	UNKNOWN    - not collectable in this scan (see "skipped" above)
//
// # Verdict caps
//
// The numeric score maps to one of four verdicts by band:
//
//	>= 90     LIKELY_UNUSED
//	70..89    CANDIDATE
//	40..69    INSPECT
//	<  40     ACTIVE
//
// Several conditions cap the verdict regardless of the numeric score:
//
//   - MISSING_SIGNAL flag present      cap = INSPECT
//   - any sub-signal evidence ESTIMATED cap = CANDIDATE
//   - COMPACTED flag present            cap = INSPECT
//   - REMOTE_STORAGE flag present       cap = INSPECT
//   - APPEARS_NEVER_USED without PURGED cap = CANDIDATE
//
// # Versioning
//
// SpecVersion follows semver. Minor versions are additive only (new flags,
// new evidence sources). Breaking changes to formulas or default weights
// require a major bump. The version string is embedded in every JSON
// snapshot produced by a conforming implementation.
//
// # License
//
// The ATTIC Score(TM) methodology specification (this package's
// constants, the doc.go text, and docs/attic-score-spec-v1.0.md) is
// licensed under CC BY 4.0. The kafka-attic implementation is licensed
// under Apache 2.0. The two licenses are intentionally distinct so that
// the methodology can be cited, forked, and re-implemented freely.
package atticspec
