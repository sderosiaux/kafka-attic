package scorer

import (
	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/types"
)

// Verdict-cap reason strings recorded in AtticScore.VerdictCappedBy. They
// match the names listed in SPEC Appendix C ("e.g., ESTIMATED_EVIDENCE,
// COMPACTED, REMOTE_STORAGE").
const (
	cappedByMissingSignal      = "MISSING_SIGNAL"
	cappedByEstimatedEvidence  = "ESTIMATED_EVIDENCE"
	cappedByCompacted          = "COMPACTED"
	cappedByRemoteStorage      = "REMOTE_STORAGE"
	cappedByAppearsNeverUsed   = "APPEARS_NEVER_USED"
)

// computeRawScore returns the weighted sum of sub-scores with skipped-weight
// redistribution per SPEC §4.2 and Appendix E.
//
// subs is keyed by SubSignal. The corresponding entry in weights drives the
// contribution unless SubScore.Skipped == true, in which case the weight is
// redistributed proportionally across the remaining (non-skipped) signals.
//
// MISSING_SIGNAL sub-signals (Activity/Tenancy/Consumption when UNKNOWN) are
// NOT skipped: they contribute their neutral 50 at their normal weight.
func computeRawScore(subs map[types.SubSignal]types.SubScore, weights types.AtticWeights) float64 {
	type entry struct {
		sig    types.SubSignal
		weight float64
		score  float64
		skip   bool
	}
	all := []entry{
		{types.SubSignalActivity, weights.Activity, 0, false},
		{types.SubSignalTenancy, weights.Tenancy, 0, false},
		{types.SubSignalTonnage, weights.Tonnage, 0, false},
		{types.SubSignalIntent, weights.Intent, 0, false},
		{types.SubSignalConsumption, weights.Consumption, 0, false},
	}
	var skippedWeight float64
	for i, e := range all {
		s, ok := subs[e.sig]
		if !ok || s.Skipped {
			all[i].skip = true
			skippedWeight += e.weight
			continue
		}
		all[i].score = float64(s.Score)
	}

	// Redistribute the skipped weight proportionally to remaining weights.
	var keptWeight float64
	for _, e := range all {
		if !e.skip {
			keptWeight += e.weight
		}
	}
	if keptWeight == 0 {
		// Every signal skipped — undefined. Return 0 conservatively.
		return 0
	}

	var raw float64
	for _, e := range all {
		if e.skip {
			continue
		}
		w := e.weight + skippedWeight*(e.weight/keptWeight)
		raw += e.score * w
	}
	return raw
}

// scoreToVerdict maps a raw score to a verdict band per SPEC §4.3 thresholds.
func scoreToVerdict(raw float64, th config.Thresholds) types.Verdict {
	score := raw
	if score >= float64(th.LikelyUnused) {
		return types.VerdictLikelyUnused
	}
	if score >= float64(th.Candidate) {
		return types.VerdictCandidate
	}
	if score >= float64(th.Inspect) {
		return types.VerdictInspect
	}
	return types.VerdictActive
}

// verdictRank assigns an ordering so we can apply caps (caps lower the
// verdict; we keep the lower of the two).
//   ACTIVE < INSPECT < CANDIDATE < LIKELY_UNUSED
func verdictRank(v types.Verdict) int {
	switch v {
	case types.VerdictActive:
		return 0
	case types.VerdictInspect:
		return 1
	case types.VerdictCandidate:
		return 2
	case types.VerdictLikelyUnused:
		return 3
	}
	return -1
}

// applyVerdictCaps enforces SPEC §4.4. It returns the (possibly lowered)
// verdict and the name of the cap rule that triggered, or empty string when
// no cap applied.
//
// Multiple caps can apply at once; we pick the strictest cap (lowest verdict)
// and record the first reason that produced that strict cap, in the order
// listed in SPEC §4.4.
//
// hasEstimated should be true when any sub-signal has evidence ESTIMATED.
// hasMissing should be true when MISSING_SIGNAL flag is present.
// flags is the topic's flag list; we test for COMPACTED, REMOTE_STORAGE,
// APPEARS_NEVER_USED here.
// hasPurged is true when PURGED flag is present (APPEARS_NEVER_USED w/o
// PURGED triggers a cap; with PURGED it does not).
func applyVerdictCaps(
	base types.Verdict,
	hasMissing bool,
	hasEstimated bool,
	flags []types.Flag,
	hasPurged bool,
) (types.Verdict, string) {
	hasFlag := func(target types.Flag) bool {
		for _, f := range flags {
			if f == target {
				return true
			}
		}
		return false
	}

	type rule struct {
		cap    types.Verdict
		name   string
		active bool
	}
	rules := []rule{
		{types.VerdictInspect, cappedByMissingSignal, hasMissing},
		{types.VerdictCandidate, cappedByEstimatedEvidence, hasEstimated},
		{types.VerdictInspect, cappedByCompacted, hasFlag(types.FlagCompacted)},
		{types.VerdictInspect, cappedByRemoteStorage, hasFlag(types.FlagRemoteStorage)},
		{types.VerdictCandidate, cappedByAppearsNeverUsed, hasFlag(types.FlagAppearsNeverUsed) && !hasPurged},
	}

	out := base
	reason := ""
	for _, r := range rules {
		if !r.active {
			continue
		}
		// A cap only fires if it would actually *lower* the verdict.
		if verdictRank(r.cap) < verdictRank(out) {
			out = r.cap
			reason = r.name
		}
	}
	return out, reason
}
