package scorer

import (
	"strings"

	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/types"
)

// scoreIntent computes the Intent sub-score per SPEC §4.2.
//
//   - SR not configured           → skipped, weight redistributed
//   - subject_strategy=record_name → skipped (cannot be determined from SR alone)
//   - SR unreachable (on_failure: warn) → skipped
//   - topic_name + subject exists  → 0
//   - topic_name + no subject     → 100 (ORPHAN_SCHEMA flag added by caller)
//   - topic_record + match        → 0
//   - topic_record + no match     → 100
//
// Returns score, evidence, skipped, ok. Skipped=true means the caller must
// redistribute the Intent weight (NO MISSING_SIGNAL — SPEC Appendix E).
func scoreIntent(
	srInfo *types.SchemaRegistryInfo,
	cfg *config.Config,
) (score int, evidence types.Evidence, skipped bool, ok bool) {
	// SR not configured at all → skipped.
	if cfg == nil || cfg.SchemaRegistry == nil {
		return 0, types.EvidenceUnknown, true, false
	}
	// Strategy from config (collector mirrors it into SchemaRegistryInfo).
	strategy := strings.TrimSpace(cfg.SchemaRegistry.SubjectStrategy)
	if strategy == "" {
		strategy = "topic_name"
	}
	if strategy == "record_name" {
		// record_name cannot be determined from SR alone → skipped.
		return 0, types.EvidenceUnknown, true, false
	}
	if srInfo == nil {
		// Collector did not populate per-topic info — treat as unreachable.
		return 0, types.EvidenceUnknown, true, false
	}
	if srInfo.Evidence == types.EvidenceUnknown {
		// SR was configured but unreachable (on_failure: warn) — skipped.
		return 0, types.EvidenceUnknown, true, false
	}

	switch strategy {
	case "topic_name", "topic_record":
		if len(srInfo.SubjectsFound) == 0 {
			return 100, types.EvidenceKnown, false, true
		}
		return 0, types.EvidenceKnown, false, true
	default:
		// Unknown strategies are treated as record_name (skipped).
		return 0, types.EvidenceUnknown, true, false
	}
}
