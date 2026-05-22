package scorer

import (
	"time"

	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/types"
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

	// ── Activity ──────────────────────────────────────────────────────────
	var activitySub types.SubScore
	if missing[types.SubSignalActivity] {
		activitySub = types.SubScore{
			Score:    neutralScore,
			Evidence: types.EvidenceUnknown,
			Input:    map[string]any{"missing_signal": true},
		}
	} else {
		ascore, aev, days, ok := scoreActivity(s.now, t.LastProduceTs, t.MessageTimestampType, curveFromConfig(cfg.AtticScore.ActivityCurve))
		if !ok {
			activitySub = types.SubScore{
				Score:    neutralScore,
				Evidence: types.EvidenceUnknown,
				Input:    map[string]any{"no_timestamp": true},
			}
			s.appendMissing(t, types.SubSignalActivity)
		} else {
			activitySub = types.SubScore{
				Score:    ascore,
				Evidence: aev,
				Input:    map[string]any{"days_since_last_produce": roundHalfUp(days)},
			}
		}
	}

	// ── Tenancy ───────────────────────────────────────────────────────────
	var tenancySub types.SubScore
	if missing[types.SubSignalTenancy] {
		tenancySub = types.SubScore{
			Score:    neutralScore,
			Evidence: types.EvidenceUnknown,
			Input:    map[string]any{"missing_signal": true},
		}
	} else {
		tscore, tev, ok := scoreTenancy(t.ConsumerGroups, t.LatestOffsetSum, true, true)
		if !ok {
			tenancySub = types.SubScore{
				Score:    neutralScore,
				Evidence: types.EvidenceUnknown,
				Input:    map[string]any{"auth_failed": true},
			}
			s.appendMissing(t, types.SubSignalTenancy)
		} else {
			tenancySub = types.SubScore{
				Score:    tscore,
				Evidence: tev,
				Input:    tenancyInput(t),
			}
		}
	}

	// ── Tonnage ───────────────────────────────────────────────────────────
	var tonnageSub types.SubScore
	{
		size := int64(0)
		if t.Storage.Bytes != nil {
			size = *t.Storage.Bytes
		}
		tnScore, tnEv, skipped, ok := scoreTonnage(size, s.clusterSize, t.Storage.Evidence)
		if skipped || !ok {
			tonnageSub = types.SubScore{
				Score:    0,
				Evidence: tnEv,
				Skipped:  true,
				Input:    map[string]any{"skipped_reason": "tonnage_unknown"},
			}
		} else {
			// percentile used (recompute for input field)
			p := percentileRank(size, s.clusterSize)
			tonnageSub = types.SubScore{
				Score:    tnScore,
				Evidence: tnEv,
				Input:    map[string]any{"percentile": p},
			}
		}
	}

	// ── Intent ────────────────────────────────────────────────────────────
	var intentSub types.SubScore
	{
		inScore, inEv, skipped, ok := scoreIntent(t.SchemaRegistry, cfg)
		if skipped || !ok {
			reason := "no_sr_configured"
			if cfg != nil && cfg.SchemaRegistry != nil {
				if cfg.SchemaRegistry.SubjectStrategy == "record_name" {
					reason = "record_name_strategy"
				} else if t.SchemaRegistry != nil && t.SchemaRegistry.Evidence == types.EvidenceUnknown {
					reason = "sr_unreachable"
				}
			}
			intentSub = types.SubScore{
				Score:    0,
				Evidence: inEv,
				Skipped:  true,
				Input:    map[string]any{"skipped_reason": reason},
			}
		} else {
			intentSub = types.SubScore{
				Score:    inScore,
				Evidence: inEv,
				Input:    map[string]any{"orphan": inScore == 100},
			}
		}
	}

	// ── Consumption ───────────────────────────────────────────────────────
	var consumptionSub types.SubScore
	if missing[types.SubSignalConsumption] {
		consumptionSub = types.SubScore{
			Score:    neutralScore,
			Evidence: types.EvidenceUnknown,
			Input:    map[string]any{"missing_signal": true},
		}
	} else {
		cscore, cev, ok := scoreConsumption(t.PartitionMetrics, false)
		if !ok {
			consumptionSub = types.SubScore{
				Score:    neutralScore,
				Evidence: types.EvidenceUnknown,
				Input:    map[string]any{"missing_signal": true},
			}
			s.appendMissing(t, types.SubSignalConsumption)
		} else {
			consumptionSub = types.SubScore{
				Score:    cscore,
				Evidence: cev,
				Input: map[string]any{
					"earliest_eq_latest": cscore == 100 || cscore == 90,
				},
			}
		}
	}

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
	for _, m := range t.SignalsMissing {
		if m == sig {
			return
		}
	}
	t.SignalsMissing = append(t.SignalsMissing, sig)
}

// roundFloat rounds a float to `digits` decimal places using half-up.
func roundFloat(v float64, digits int) float64 {
	mult := 1.0
	for i := 0; i < digits; i++ {
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

// tenancyInput summarises the per-topic group state into the SubScore.Input
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
