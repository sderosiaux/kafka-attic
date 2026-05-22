package scorer

import (
	"strings"

	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/types"
)

// flagInputs bundles every signal the flag computation needs. Built by
// scorer.go from the topic + the per-sub-signal results, so flags.go can stay
// a pure function over a small struct.
type flagInputs struct {
	Topic             *types.Topic
	IntentSkipped     bool
	IntentScore       int
	TonnageEvidence   types.Evidence
	TonnageSkipped    bool
	PartitionAuthFail bool
	GroupsHaveCommit  bool // any group has committed_offset > 0
	StrategyOK        bool // topic-derived strategy (topic_name / topic_record)
	MetricsConfigured bool // cfg.Metrics != nil → OVERSIZED can be emitted
	OversizedFlagged  bool // pre-computed elsewhere; default false here
	SkewedFlagged     bool // pre-computed elsewhere; default false here
}

// computeFlags returns the per-topic flag set per SPEC §4.5 and Appendix D.
//
// Structural flags COMPACTED and REMOTE_STORAGE come from the collector
// (Topic.Flags already contains them). MISSING_SIGNAL likewise: the collector
// populates Topic.SignalsMissing. The scorer's job here is the *derived*
// flags: APPEARS_NEVER_USED, PURGED, ORPHAN_SCHEMA, plus passing through the
// SKEWED/OVERSIZED markers the caller decides on.
//
// The output is the *deduplicated* combined set, in stable order.
func computeFlags(in flagInputs, cfg *config.Config) []types.Flag {
	t := in.Topic
	if t == nil {
		return nil
	}

	set := map[types.Flag]struct{}{}
	for _, f := range t.Flags {
		set[f] = struct{}{}
	}

	// APPEARS_NEVER_USED — single-scan evidence per SPEC §4.5:
	//   earliest == latest == 0 across all partitions
	//   AND no consumer group has ever committed (committed_offset_sum == 0
	//   for every group, i.e. !GroupsHaveCommit)
	if neverUsed(t) && !in.GroupsHaveCommit {
		set[types.FlagAppearsNeverUsed] = struct{}{}
	}

	// PURGED — earliest > 0 AND earliest == latest. We only check the
	// per-partition view since the sums can mask the pattern.
	if purged(t) {
		set[types.FlagPurged] = struct{}{}
	}

	// ORPHAN_SCHEMA — Intent sub-signal returned 100 (no subject) AND the
	// strategy is topic-derived (topic_name / topic_record). The skipped case
	// (record_name or SR unreachable) MUST NOT emit ORPHAN_SCHEMA.
	if in.StrategyOK && !in.IntentSkipped && in.IntentScore == 100 {
		set[types.FlagOrphanSchema] = struct{}{}
	}

	// OVERSIZED — SPEC §4.5: requires a metrics source. Without metrics,
	// never emit the flag. With metrics, the caller pre-computes whether the
	// thresholds are exceeded (we don't have the metric values here) and
	// passes OversizedFlagged=true. We additionally enforce the
	// cfg.Oversized.RequiresMetrics gate.
	if in.OversizedFlagged && in.MetricsConfigured {
		if cfg == nil || cfg.Oversized == nil || !cfg.Oversized.RequiresMetrics || in.MetricsConfigured {
			set[types.FlagOversized] = struct{}{}
		}
	}

	// SKEWED — emitted only when partition sizes are known (per SPEC §4.5
	// implicit dependency on size_bytes). Caller is responsible for the ratio
	// computation; we trust SkewedFlagged.
	if in.SkewedFlagged && partitionSizesKnown(t) {
		set[types.FlagSkewed] = struct{}{}
	}

	// MISSING_SIGNAL — emitted when any sub-signal is UNKNOWN that maps to
	// MISSING_SIGNAL per SPEC Appendix E (Activity, Tenancy, Consumption).
	// The signals_missing list on the topic drives this.
	if len(t.SignalsMissing) > 0 {
		set[types.FlagMissingSignal] = struct{}{}
	}

	// Stable order (matches the order in SPEC Appendix D).
	order := []types.Flag{
		types.FlagAppearsNeverUsed,
		types.FlagPurged,
		types.FlagOversized,
		types.FlagSkewed,
		types.FlagOrphanSchema,
		types.FlagCompacted,
		types.FlagRemoteStorage,
		types.FlagMissingSignal,
	}
	out := make([]types.Flag, 0, len(set))
	for _, f := range order {
		if _, ok := set[f]; ok {
			out = append(out, f)
		}
	}
	return out
}

// neverUsed returns true when every partition has earliest == latest == 0.
// SPEC §4.5 APPEARS_NEVER_USED definition.
func neverUsed(t *types.Topic) bool {
	if len(t.PartitionMetrics) == 0 {
		return false
	}
	for _, p := range t.PartitionMetrics {
		if p.EarliestOffset != 0 || p.LatestOffset != 0 {
			return false
		}
	}
	return true
}

// purged returns true when every partition has earliest == latest AND at
// least one partition has earliest > 0.
func purged(t *types.Topic) bool {
	if len(t.PartitionMetrics) == 0 {
		return false
	}
	anyGt := false
	for _, p := range t.PartitionMetrics {
		if p.EarliestOffset != p.LatestOffset {
			return false
		}
		if p.EarliestOffset > 0 {
			anyGt = true
		}
	}
	return anyGt
}

// partitionSizesKnown reports whether every partition has SizeBytes populated
// (non-nil). SKEWED is only safe to emit when sizes are observable.
func partitionSizesKnown(t *types.Topic) bool {
	if len(t.PartitionMetrics) == 0 {
		return false
	}
	for _, p := range t.PartitionMetrics {
		if p.SizeBytes == nil {
			return false
		}
	}
	return true
}

// groupsHaveCommit returns true when any consumer group has ever committed an
// offset (committed_offset_sum > 0). Required for APPEARS_NEVER_USED.
func groupsHaveCommit(t *types.Topic) bool {
	for _, g := range t.ConsumerGroups {
		if g.CommittedOffsetSum > 0 {
			return true
		}
	}
	return false
}

// strategyOK reports whether the SR strategy is topic-derived (topic_name or
// topic_record). Used to gate ORPHAN_SCHEMA.
func strategyOK(cfg *config.Config) bool {
	if cfg == nil || cfg.SchemaRegistry == nil {
		return false
	}
	s := strings.TrimSpace(cfg.SchemaRegistry.SubjectStrategy)
	return s == "" || s == "topic_name" || s == "topic_record"
}

// metricsConfigured reports whether a metrics source is configured. Used to
// gate OVERSIZED (SPEC §4.5: never emitted without metrics).
func metricsConfigured(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}
	return cfg.Metrics != nil
}
