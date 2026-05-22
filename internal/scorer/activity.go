// Package scorer implements the ATTIC Score (SPEC §4). Pure logic, no I/O:
// inputs are the *types.Snapshot and *types.Topic populated by the collector,
// outputs are sub-scores, the raw_score, the verdict, and the flag list.
package scorer

import (
	"sort"
	"time"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

// neutralScore is the value assigned to a MISSING_SIGNAL sub-signal so it
// contributes nothing biased to the raw score. SPEC Appendix E: Activity,
// Tenancy, Consumption use neutral 50 + MISSING_SIGNAL flag when UNKNOWN.
const neutralScore = 50

// scoreActivity computes the Activity sub-score (SPEC §4.2).
//
// The curve is piecewise-linear over (days, score) anchors sorted ascending by
// days. Inputs:
//   - now: the scan time (defaults to time.Now() when zero)
//   - lastProduceTS: nil when no broker timestamp was returned
//   - timestampType: "LogAppendTime", "CreateTime", or "" (unknown)
//   - curve: from cfg.AtticScore.ActivityCurve, monotonic days, monotonic score
//
// Evidence per SPEC §5.1:
//   - LogAppendTime + ts present → KNOWN
//   - CreateTime + ts present → ESTIMATED
//   - no ts → UNKNOWN (caller flags MISSING_SIGNAL and uses neutral 50)
func scoreActivity(
	now time.Time,
	lastProduceTS *time.Time,
	timestampType string,
	curve []types.ActivityCurvePoint,
) (score int, evidence types.Evidence, days float64, ok bool) {
	if lastProduceTS == nil {
		// No broker timestamp → caller treats as MISSING_SIGNAL + neutral 50.
		return neutralScore, types.EvidenceUnknown, 0, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	delta := max(now.Sub(*lastProduceTS), 0)
	days = delta.Hours() / 24.0
	score = interpolateCurve(days, curve)
	switch timestampType {
	case "LogAppendTime":
		evidence = types.EvidenceKnown
	case "CreateTime", "":
		// Default Kafka behavior is CreateTime when unset; we treat the empty
		// string the same way because a broker that returned a timestamp but
		// failed to report message.timestamp.type cannot be trusted to be
		// LogAppendTime. SPEC §5.1 reads conservatively.
		evidence = types.EvidenceEstimated
	default:
		evidence = types.EvidenceEstimated
	}
	return score, evidence, days, true
}

// interpolateCurve does piecewise-linear interpolation against the curve.
//
// The curve must be non-empty. We sort defensively in case the caller passed
// an unsorted slice. Below the first anchor → first anchor's score. Above the
// last anchor → last anchor's score. Between anchors → linear interpolation.
//
// We round to nearest int. Half-up rounding so 91.6 → 92 (matches SPEC §4.6
// worked example where d=287 yields 91.57 ≈ 92).
func interpolateCurve(days float64, curve []types.ActivityCurvePoint) int {
	if len(curve) == 0 {
		return neutralScore
	}
	// Defensive copy + sort by days ascending.
	sorted := make([]types.ActivityCurvePoint, len(curve))
	copy(sorted, curve)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Days < sorted[j].Days })

	if days <= float64(sorted[0].Days) {
		return sorted[0].Score
	}
	last := sorted[len(sorted)-1]
	if days >= float64(last.Days) {
		return last.Score
	}
	for i := 1; i < len(sorted); i++ {
		hi := sorted[i]
		if days <= float64(hi.Days) {
			lo := sorted[i-1]
			span := float64(hi.Days - lo.Days)
			if span <= 0 {
				return hi.Score
			}
			frac := (days - float64(lo.Days)) / span
			val := float64(lo.Score) + frac*float64(hi.Score-lo.Score)
			return roundHalfUp(val)
		}
	}
	return last.Score
}

// roundHalfUp rounds to nearest int with .5 rounding away from zero. Used so
// the SPEC §4.6 worked example (91.57 → 92) lines up.
func roundHalfUp(v float64) int {
	if v >= 0 {
		return int(v + 0.5)
	}
	return int(v - 0.5)
}
