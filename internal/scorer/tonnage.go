package scorer

import (
	"slices"
	"sort"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

// scoreTonnage computes per-topic Tonnage score from the cluster-wide size
// distribution per SPEC §4.2.
//
//	tonnage_score = max(0, 100 - p)
//
// where p ∈ [0, 100] is the percentile rank of this topic's storage size
// across the cluster's *KNOWN/ESTIMATED* topics. Smaller topics → higher
// score. UNKNOWN tonnage → skipped (caller redistributes weight, NO
// MISSING_SIGNAL per SPEC Appendix E).
//
// Inputs:
//   - sizeBytes: this topic's storage in bytes
//   - sortedSizes: every other topic's KNOWN/ESTIMATED storage size, sorted
//     ascending. Caller is responsible for sorting once per snapshot for
//     O(log n) per topic.
//   - storageEvidence: collector's evidence on Storage block (KNOWN/ESTIMATED/UNKNOWN)
//
// Returns score, evidence, skipped, ok. When evidence is UNKNOWN, skipped=true
// and ok=false (caller redistributes weight).
func scoreTonnage(
	sizeBytes int64,
	sortedSizes []int64,
	storageEvidence types.Evidence,
) (score int, evidence types.Evidence, skipped bool, ok bool) {
	if storageEvidence == types.EvidenceUnknown {
		return 0, types.EvidenceUnknown, true, false
	}
	p := percentileRank(sizeBytes, sortedSizes)
	score = min(max(100-p, 0), 100)
	return score, storageEvidence, false, true
}

// percentileRank returns the percentile rank (0–100) of v among xs.
//
// Definition (matches SPEC §4.6 worked example where p=4 yields 96):
//
//	p = round( count(x < v) / N × 100 )
//
// xs must be sorted ascending. We use sort.Search to find the position of the
// first element >= v in O(log N).
//
// Edge cases:
//   - empty xs → 0
//   - v smaller than every xs → 0
//   - v larger than every xs → close to 100
func percentileRank(v int64, xs []int64) int {
	n := len(xs)
	if n == 0 {
		return 0
	}
	// Count of strictly-smaller elements: first index where xs[i] >= v.
	idx := sort.Search(n, func(i int) bool { return xs[i] >= v })
	frac := float64(idx) / float64(n)
	return roundHalfUp(frac * 100.0)
}

// sortedClusterSizes extracts every topic's storage size whose evidence is
// KNOWN or ESTIMATED from the snapshot, sorted ascending. Used as the
// percentile-rank base for every per-topic call.
//
// We exclude UNKNOWN storage because SPEC §4.2 Tonnage says: when UNKNOWN, the
// sub-signal is skipped (the topic does not contribute to the distribution).
func sortedClusterSizes(snap *types.Snapshot) []int64 {
	if snap == nil {
		return nil
	}
	out := make([]int64, 0, len(snap.Topics))
	for _, t := range snap.Topics {
		if t.Storage.Bytes == nil {
			continue
		}
		if t.Storage.Evidence == types.EvidenceUnknown {
			continue
		}
		out = append(out, *t.Storage.Bytes)
	}
	slices.Sort(out)
	return out
}
