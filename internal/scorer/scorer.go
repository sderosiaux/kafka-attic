package scorer

import (
	"slices"
	"time"

	"github.com/sderosiaux/kafka-attic/internal/config"
	"github.com/sderosiaux/kafka-attic/internal/types"
)

// Scorer is the stateful per-snapshot scorer. It caches the
// cluster-wide sorted size distribution (needed for Tonnage percentile rank)
// so we compute it once per scan rather than once per topic.
type Scorer struct {
	cfg         *config.Config
	now         time.Time
	clusterSize []int64 // ascending, KNOWN+ESTIMATED only
}

// New returns a fresh Scorer bound to a config and snapshot. The snapshot is
// only used to precompute the size distribution. The `now` argument is used
// for Activity age math; pass time.Time{} for time.Now().UTC().
func New(cfg *config.Config, snap *types.Snapshot, now time.Time) *Scorer {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return &Scorer{
		cfg:         cfg,
		now:         now,
		clusterSize: sortedClusterSizes(snap),
	}
}

// Score computes the per-topic ATTIC score and mutates the topic in-place,
// populating t.Attic, t.Flags, and (when applicable) t.SignalsMissing.
//
// The function is idempotent: callers may invoke it multiple times against
// the same topic without producing different output, provided the inputs
// haven't changed.
func (s *Scorer) Score(_ *types.Snapshot, t *types.Topic) {
	if t == nil || s == nil {
		return
	}
	cfg := s.cfg

	// Helpers translating collector's SignalsMissing → per-signal degraded
	// permissions. The collector adds a SubSignal entry whenever the relevant
	// admin API failed (offsets.go: partition auth → Consumption; groups: →
	// Tenancy; offsets last-ts: → Activity).
	missing := map[types.SubSignal]bool{}
	for _, sm := range t.SignalsMissing {
		missing[sm] = true
	}

	activitySub := s.scoreActivitySub(t, cfg, missing)
	tenancySub := s.scoreTenancySub(t, missing)
	tonnageSub := s.scoreTonnageSub(t)
	intentSub := s.scoreIntentSub(t, cfg)
	consumptionSub := s.scoreConsumptionSub(t, missing)

	subs := map[types.SubSignal]types.SubScore{
		types.SubSignalActivity:    activitySub,
		types.SubSignalTenancy:     tenancySub,
		types.SubSignalTonnage:     tonnageSub,
		types.SubSignalIntent:      intentSub,
		types.SubSignalConsumption: consumptionSub,
	}

	weights := types.AtticWeights{
		Activity:    cfg.AtticScore.Weights.Activity,
		Tenancy:     cfg.AtticScore.Weights.Tenancy,
		Tonnage:     cfg.AtticScore.Weights.Tonnage,
		Intent:      cfg.AtticScore.Weights.Intent,
		Consumption: cfg.AtticScore.Weights.Consumption,
	}

	raw := computeRawScore(subs, weights)

	// ── Flags ─────────────────────────────────────────────────────────────
	fin := flagInputs{
		Topic:             t,
		IntentSkipped:     intentSub.Skipped,
		IntentScore:       intentSub.Score,
		TonnageEvidence:   tonnageSub.Evidence,
		TonnageSkipped:    tonnageSub.Skipped,
		PartitionAuthFail: missing[types.SubSignalConsumption],
		GroupsHaveCommit:  groupsHaveCommit(t),
		StrategyOK:        strategyOK(cfg),
		MetricsConfigured: metricsConfigured(cfg),
	}
	t.Flags = computeFlags(fin, cfg)

	// ── Verdict + caps ────────────────────────────────────────────────────
	thresholds := config.Thresholds{
		LikelyUnused: cfg.AtticScore.Thresholds.LikelyUnused,
		Candidate:    cfg.AtticScore.Thresholds.Candidate,
		Inspect:      cfg.AtticScore.Thresholds.Inspect,
	}
	base := scoreToVerdict(raw, thresholds)

	hasMissing := false
	hasEstimated := false
	hasPurged := false
	for _, f := range t.Flags {
		if f == types.FlagMissingSignal {
			hasMissing = true
		}
		if f == types.FlagPurged {
			hasPurged = true
		}
	}
	// hasMissing covers both flag presence and any sub-signal evidence == UNKNOWN
	// that isn't skipped (Activity/Tenancy/Consumption per SPEC Appendix E).
	for _, sub := range subs {
		if sub.Skipped {
			continue
		}
		if sub.Evidence == types.EvidenceEstimated {
			hasEstimated = true
		}
	}

	final, cappedBy := applyVerdictCaps(base, hasMissing, hasEstimated, t.Flags, hasPurged)

	var cappedPtr *string
	if cappedBy != "" {
		r := cappedBy
		cappedPtr = &r
	}

	t.Attic = types.AtticScore{
		SpecVersion:     types.AtticSpecVersion,
		SubScores:       subs,
		RawScore:        roundFloat(raw, 1),
		Verdict:         final,
		VerdictCappedBy: cappedPtr,
	}
}

// appendMissing adds a SubSignal to t.SignalsMissing if not already present.
func (s *Scorer) appendMissing(t *types.Topic, sig types.SubSignal) {
	if slices.Contains(t.SignalsMissing, sig) {
		return
	}
	t.SignalsMissing = append(t.SignalsMissing, sig)
}

// missingSubScore builds the SubScore returned when a sub-signal is flagged
// MISSING_SIGNAL by the collector (or by Score itself).
func missingSubScore(reason string) types.SubScore {
	return types.SubScore{
		Score:    neutralScore,
		Evidence: types.EvidenceUnknown,
		Input:    map[string]any{reason: true},
	}
}

// scoreActivitySub computes the Activity sub-score for t.
func (s *Scorer) scoreActivitySub(t *types.Topic, cfg *config.Config, missing map[types.SubSignal]bool) types.SubScore {
	if missing[types.SubSignalActivity] {
		return missingSubScore("missing_signal")
	}
	ascore, aev, days, ok := scoreActivity(s.now, t.LastProduceTS, t.MessageTimestampType, curveFromConfig(cfg.AtticScore.ActivityCurve))
	if !ok {
		s.appendMissing(t, types.SubSignalActivity)
		return missingSubScore("no_timestamp")
	}
	return types.SubScore{
		Score:    ascore,
		Evidence: aev,
		Input:    map[string]any{"days_since_last_produce": roundHalfUp(days)},
	}
}

// scoreTenancySub computes the Tenancy sub-score for t.
func (s *Scorer) scoreTenancySub(t *types.Topic, missing map[types.SubSignal]bool) types.SubScore {
	if missing[types.SubSignalTenancy] {
		return missingSubScore("missing_signal")
	}
	tscore, tev, ok := scoreTenancy(t.ConsumerGroups, t.LatestOffsetSum, true, true)
	if !ok {
		s.appendMissing(t, types.SubSignalTenancy)
		return missingSubScore("auth_failed")
	}
	return types.SubScore{
		Score:    tscore,
		Evidence: tev,
		Input:    tenancyInput(t),
	}
}

// scoreTonnageSub computes the Tonnage sub-score for t.
func (s *Scorer) scoreTonnageSub(t *types.Topic) types.SubScore {
	size := int64(0)
	if t.Storage.Bytes != nil {
		size = *t.Storage.Bytes
	}
	tnScore, tnEv, skipped, ok := scoreTonnage(size, s.clusterSize, t.Storage.Evidence)
	if skipped || !ok {
		return types.SubScore{
			Score:    0,
			Evidence: tnEv,
			Skipped:  true,
			Input:    map[string]any{"skipped_reason": "tonnage_unknown"},
		}
	}
	p := percentileRank(size, s.clusterSize)
	return types.SubScore{
		Score:    tnScore,
		Evidence: tnEv,
		Input:    map[string]any{"percentile": p},
	}
}

// scoreIntentSub computes the Intent sub-score for t.
func (s *Scorer) scoreIntentSub(t *types.Topic, cfg *config.Config) types.SubScore {
	inScore, inEv, skipped, ok := scoreIntent(t.SchemaRegistry, cfg)
	if skipped || !ok {
		reason := "no_sr_configured"
		if cfg != nil && cfg.SchemaRegistry != nil {
			switch {
			case cfg.SchemaRegistry.SubjectStrategy == "record_name":
				reason = "record_name_strategy"
			case t.SchemaRegistry != nil && t.SchemaRegistry.Evidence == types.EvidenceUnknown:
				reason = "sr_unreachable"
			}
		}
		return types.SubScore{
			Score:    0,
			Evidence: inEv,
			Skipped:  true,
			Input:    map[string]any{"skipped_reason": reason},
		}
	}
	return types.SubScore{
		Score:    inScore,
		Evidence: inEv,
		Input:    map[string]any{"orphan": inScore == 100},
	}
}

// scoreConsumptionSub computes the Consumption sub-score for t.
func (s *Scorer) scoreConsumptionSub(t *types.Topic, missing map[types.SubSignal]bool) types.SubScore {
	if missing[types.SubSignalConsumption] {
		return missingSubScore("missing_signal")
	}
	cscore, cev, ok := scoreConsumption(t.PartitionMetrics, false)
	if !ok {
		s.appendMissing(t, types.SubSignalConsumption)
		return missingSubScore("missing_signal")
	}
	return types.SubScore{
		Score:    cscore,
		Evidence: cev,
		Input: map[string]any{
			"earliest_eq_latest": cscore == 100 || cscore == 90,
		},
	}
}

// roundFloat rounds a float to `digits` decimal places using half-up.
func roundFloat(v float64, digits int) float64 {
	mult := 1.0
	for range digits {
		mult *= 10
	}
	if v >= 0 {
		return float64(int(v*mult+0.5)) / mult
	}
	return float64(int(v*mult-0.5)) / mult
}

// curveFromConfig converts the config ActivityCurvePoint slice (Score is
// float for config flexibility) into the types ActivityCurvePoint slice
// (Score is int) that the interpolation helper expects.
func curveFromConfig(in []config.ActivityCurvePoint) []types.ActivityCurvePoint {
	out := make([]types.ActivityCurvePoint, 0, len(in))
	for _, p := range in {
		out = append(out, types.ActivityCurvePoint{Days: p.Days, Score: int(p.Score + 0.5)})
	}
	return out
}

// tenancyInput summarizes the per-topic group state into the SubScore.Input
// field per SPEC Appendix C example.
func tenancyInput(t *types.Topic) map[string]any {
	if len(t.ConsumerGroups) == 0 {
		return map[string]any{"no_groups": true}
	}
	allDead := true
	for _, g := range t.ConsumerGroups {
		if g.State != groupDead {
			allDead = false
			break
		}
	}
	if allDead {
		return map[string]any{"all_groups_dead": true}
	}
	return map[string]any{"group_count": len(t.ConsumerGroups)}
}
