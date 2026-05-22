package scorer

import (
	"testing"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

func parts(values ...[2]int64) []types.PartitionMetric {
	out := make([]types.PartitionMetric, 0, len(values))
	for i, v := range values {
		out = append(out, types.PartitionMetric{
			Partition:      int32(i),
			EarliestOffset: v[0],
			LatestOffset:   v[1],
		})
	}
	return out
}

func TestConsumption_NeverUsed(t *testing.T) {
	ps := parts([2]int64{0, 0}, [2]int64{0, 0})
	score, ev, ok := scoreConsumption(ps, false)
	if score != 100 || ev != types.EvidenceKnown || !ok {
		t.Errorf("got %d %v %v want 100 KNOWN true", score, ev, ok)
	}
}

func TestConsumption_Purged(t *testing.T) {
	ps := parts([2]int64{50, 50}, [2]int64{120, 120})
	score, _, _ := scoreConsumption(ps, false)
	if score != 90 {
		t.Errorf("got %d want 90 (purged)", score)
	}
}

func TestConsumption_RecordsPresent(t *testing.T) {
	ps := parts([2]int64{0, 50}, [2]int64{10, 90})
	score, _, _ := scoreConsumption(ps, false)
	if score != 0 {
		t.Errorf("got %d want 0 (records present)", score)
	}
}

func TestConsumption_AuthFail_UnknownMissingSignal(t *testing.T) {
	ps := parts([2]int64{0, 0})
	score, ev, ok := scoreConsumption(ps, true)
	if score != neutralScore || ev != types.EvidenceUnknown || ok {
		t.Errorf("got %d %v %v want %d UNKNOWN false", score, ev, ok, neutralScore)
	}
}

func TestConsumption_MixedPurgedAndEmpty(t *testing.T) {
	// One partition purged (5,5), one empty (0,0). All equal, max earliest>0
	// → still classified PURGED.
	ps := parts([2]int64{0, 0}, [2]int64{5, 5})
	score, _, _ := scoreConsumption(ps, false)
	if score != 90 {
		t.Errorf("got %d want 90 (mixed empty+purged)", score)
	}
}
