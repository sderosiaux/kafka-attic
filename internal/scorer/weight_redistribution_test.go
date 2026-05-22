package scorer

import (
	"math"
	"testing"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

// When two sub-signals (Tonnage + Intent) are skipped, the remaining three
// weights must be normalized to sum to 1.0. We verify this indirectly: if
// every remaining sub-score is the same value X, the raw score must equal X.
func TestWeightRedistribution_TonnageAndIntentSkipped(t *testing.T) {
	weights := types.AtticWeights{
		Activity: 0.30, Tenancy: 0.20, Tonnage: 0.10, Intent: 0.15, Consumption: 0.25,
	}
	subs := map[types.SubSignal]types.SubScore{
		types.SubSignalActivity:    {Score: 80, Evidence: types.EvidenceKnown},
		types.SubSignalTenancy:     {Score: 80, Evidence: types.EvidenceKnown},
		types.SubSignalTonnage:     {Score: 0, Evidence: types.EvidenceUnknown, Skipped: true},
		types.SubSignalIntent:      {Score: 0, Evidence: types.EvidenceUnknown, Skipped: true},
		types.SubSignalConsumption: {Score: 80, Evidence: types.EvidenceKnown},
	}
	raw := computeRawScore(subs, weights)
	if math.Abs(raw-80.0) > 0.001 {
		t.Errorf("raw=%v want 80 (all remaining at 80 → redistributed weights sum to 1)", raw)
	}

	// Check explicit redistribution proportions: kept weights are
	// 0.30/0.20/0.25, summing to 0.75; the skipped 0.25 (0.10 + 0.15)
	// redistributes proportionally so the kept weights become
	//   activity:    0.30 + 0.25 × (0.30/0.75) = 0.40
	//   tenancy:     0.20 + 0.25 × (0.20/0.75) ≈ 0.2667
	//   consumption: 0.25 + 0.25 × (0.25/0.75) ≈ 0.3333
	//   sum: 1.0
	subs[types.SubSignalActivity] = types.SubScore{Score: 100, Evidence: types.EvidenceKnown}
	subs[types.SubSignalTenancy] = types.SubScore{Score: 0, Evidence: types.EvidenceKnown}
	subs[types.SubSignalConsumption] = types.SubScore{Score: 0, Evidence: types.EvidenceKnown}
	raw = computeRawScore(subs, weights)
	if math.Abs(raw-40.0) > 0.001 {
		t.Errorf("raw=%v want 40 (only activity scores 100 at weight 0.40)", raw)
	}
}

// When only Tonnage is skipped, redistribution must still sum to 1.0.
func TestWeightRedistribution_OnlyTonnageSkipped(t *testing.T) {
	weights := types.AtticWeights{
		Activity: 0.30, Tenancy: 0.20, Tonnage: 0.10, Intent: 0.15, Consumption: 0.25,
	}
	subs := map[types.SubSignal]types.SubScore{
		types.SubSignalActivity:    {Score: 50},
		types.SubSignalTenancy:     {Score: 50},
		types.SubSignalTonnage:     {Score: 0, Skipped: true},
		types.SubSignalIntent:      {Score: 50},
		types.SubSignalConsumption: {Score: 50},
	}
	raw := computeRawScore(subs, weights)
	if math.Abs(raw-50.0) > 0.001 {
		t.Errorf("raw=%v want 50", raw)
	}
}
