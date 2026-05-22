package scorer

import (
	"testing"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

func TestPercentileRank(t *testing.T) {
	xs := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	cases := []struct {
		v    int64
		want int
	}{
		{0, 0},
		{1, 0},
		{2, 10}, // 1 strictly smaller out of 10 → 10
		{5, 40},
		{10, 90},
		{11, 100},
	}
	for _, c := range cases {
		got := percentileRank(c.v, xs)
		if got != c.want {
			t.Errorf("percentileRank(%d)=%d want %d", c.v, got, c.want)
		}
	}
}

func TestScoreTonnage_KnownSmallTopicHighScore(t *testing.T) {
	// SPEC §4.6: 12.3 GB at p4 → score 96. We model this with a base of 25
	// equally-spaced sizes and put our topic at the 4th percentile.
	sizes := make([]int64, 25)
	for i := range sizes {
		sizes[i] = int64(i+1) * 1_000_000_000
	}
	// Place our topic strictly between sizes[0] and sizes[1] → 1 strictly
	// smaller → percentile = 4 → score = 96.
	got, ev, skipped, ok := scoreTonnage(1_500_000_000, sizes, types.EvidenceKnown)
	if skipped || !ok {
		t.Fatalf("not skipped expected")
	}
	if got != 96 {
		t.Errorf("got %d want 96", got)
	}
	if ev != types.EvidenceKnown {
		t.Errorf("evidence %v want KNOWN", ev)
	}
}

func TestScoreTonnage_UnknownSkipped(t *testing.T) {
	got, ev, skipped, ok := scoreTonnage(123, nil, types.EvidenceUnknown)
	if !skipped || ok {
		t.Errorf("expected skipped=true ok=false; got %v %v", skipped, ok)
	}
	if got != 0 {
		t.Errorf("score=%d want 0 on skip", got)
	}
	if ev != types.EvidenceUnknown {
		t.Errorf("evidence=%v want UNKNOWN", ev)
	}
}

func TestScoreTonnage_RedistributesWeight(t *testing.T) {
	// Synthesize a SubScores map with Tonnage skipped and verify the rest of
	// the weight redistributes proportionally.
	weights := types.AtticWeights{
		Activity: 0.30, Tenancy: 0.20, Tonnage: 0.10, Intent: 0.15, Consumption: 0.25,
	}
	subs := map[types.SubSignal]types.SubScore{
		types.SubSignalActivity:    {Score: 100, Evidence: types.EvidenceKnown},
		types.SubSignalTenancy:     {Score: 100, Evidence: types.EvidenceKnown},
		types.SubSignalTonnage:     {Score: 0, Evidence: types.EvidenceUnknown, Skipped: true},
		types.SubSignalIntent:      {Score: 100, Evidence: types.EvidenceKnown},
		types.SubSignalConsumption: {Score: 100, Evidence: types.EvidenceKnown},
	}
	raw := computeRawScore(subs, weights)
	// All non-skipped scores are 100 → total weight after redistribution must
	// also sum to 1.0 → raw must equal 100.
	if raw < 99.999 || raw > 100.001 {
		t.Errorf("raw=%v want ~100 after redistribution", raw)
	}
}

func TestSortedClusterSizes_ExcludesUnknown(t *testing.T) {
	b1 := int64(100)
	b2 := int64(50)
	b3 := int64(999)
	snap := &types.Snapshot{
		Topics: []types.Topic{
			{Storage: types.StorageInfo{Bytes: &b1, Evidence: types.EvidenceKnown}},
			{Storage: types.StorageInfo{Bytes: &b2, Evidence: types.EvidenceEstimated}},
			{Storage: types.StorageInfo{Bytes: &b3, Evidence: types.EvidenceUnknown}},
			{Storage: types.StorageInfo{Bytes: nil, Evidence: types.EvidenceUnknown}},
		},
	}
	got := sortedClusterSizes(snap)
	if len(got) != 2 {
		t.Fatalf("len=%d want 2", len(got))
	}
	if got[0] != 50 || got[1] != 100 {
		t.Errorf("sorted=%v want [50 100]", got)
	}
}
