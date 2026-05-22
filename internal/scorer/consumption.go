package scorer

import (
	"github.com/sderosiaux/kafka-attic/internal/types"
)

// scoreConsumption implements SPEC §4.2 Consumption:
//
//	earliest == latest == 0 across partitions  → 100  (never used)
//	earliest > 0 && earliest == latest          → 90  (PURGED)
//	otherwise                                   →  0  (records present)
//
// Evidence is KNOWN unless any partition's ListOffsets failed (caller passes
// partitionAuthFailed=true) → UNKNOWN + neutral 50 + MISSING_SIGNAL.
//
// `parts` MUST be the full partition list for the topic. We aggregate
// per-partition rather than relying on EarliestOffsetSum/LatestOffsetSum
// because a sum of {0, 5} equals a sum of {5, 0} — both would look the same
// but only the second case is "records present, some partitions purged".
//
// Decision rule (per-partition):
//   - If every partition has earliest == latest, the topic has no live
//     records. Subdivide:
//     earliest == 0 across all  → 100 (never used)
//     earliest > 0 anywhere     →  90 (purged)
//   - Otherwise at least one partition has earliest < latest → records
//     present → 0.
func scoreConsumption(
	parts []types.PartitionMetric,
	partitionAuthFailed bool,
) (score int, evidence types.Evidence, ok bool) {
	if partitionAuthFailed {
		return neutralScore, types.EvidenceUnknown, false
	}
	if len(parts) == 0 {
		// No partitions visible at all → treat as never used. (An empty topic
		// list is impossible in Kafka, but defensively this maps to "appears
		// never used".)
		return 100, types.EvidenceKnown, true
	}
	allEqual := true
	anyEarliestGtZero := false
	for _, p := range parts {
		if p.EarliestOffset != p.LatestOffset {
			allEqual = false
			break
		}
		if p.EarliestOffset > 0 {
			anyEarliestGtZero = true
		}
	}
	if !allEqual {
		return 0, types.EvidenceKnown, true
	}
	if anyEarliestGtZero {
		return 90, types.EvidenceKnown, true
	}
	return 100, types.EvidenceKnown, true
}
