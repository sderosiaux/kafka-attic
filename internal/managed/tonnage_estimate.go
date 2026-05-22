package managed

// EstimateInput carries the broker-reported segment metadata needed to
// estimate a topic's storage when DescribeLogDirs is restricted. Every
// field comes from log-segment metadata or offset deltas; **no record is
// ever read** (SPEC §5.6).
type EstimateInput struct {
	// SegmentBytes is the total bytes reported by the broker across all
	// segments for this topic (sum of segment file sizes). Comes from log
	// segment metadata; zero means "the broker did not return any usable
	// segment metadata" and the estimate yields UNKNOWN.
	SegmentBytes int64

	// SegmentRecordCount is the total record count across the same set of
	// segments. SPEC §5.6 spells the input out explicitly: average record
	// size = SegmentBytes / SegmentRecordCount, computed broker-side.
	// Zero or negative means "no usable record count" → UNKNOWN.
	SegmentRecordCount int64

	// EarliestOffsetSum is the sum of partition earliest offsets for the
	// topic (post-retention low watermark). Used to derive how many
	// records currently live on the topic without re-reading them.
	EarliestOffsetSum int64

	// LatestOffsetSum is the sum of partition end offsets. Same caveat.
	LatestOffsetSum int64
}

// EstimateResult is the outcome of a tonnage estimate. When OK is false,
// the caller must treat Tonnage as UNKNOWN per SPEC §4.2 and skip the
// sub-signal (with weight redistribution, see SPEC Appendix E).
type EstimateResult struct {
	// Bytes is the estimated on-disk bytes for the topic. Only meaningful
	// when OK is true.
	Bytes int64

	// AvgRecordBytes is the broker-reported average record size used in
	// the estimate. Exposed for transparency in the per-topic JSON.
	AvgRecordBytes float64

	// OK is true when every input was sufficient to produce an estimate.
	OK bool

	// Reason is a short human-readable explanation for why the estimate
	// could not be produced (only set when OK is false).
	Reason string
}

// EstimateTonnage applies the SPEC §5.6 formula:
//
//	avg_record_size = SegmentBytes / SegmentRecordCount
//	bytes           = (LatestOffsetSum - EarliestOffsetSum) × avg_record_size
//
// All inputs must be derived from broker-reported segment metadata; the
// function does not, cannot, and will never sample records. When any input
// is missing or non-positive, the function returns OK=false with a Reason.
//
// The estimate is intentionally pessimistic on edge cases:
//
//   - SegmentRecordCount == 0 → UNKNOWN (can't divide).
//   - LatestOffsetSum <= EarliestOffsetSum → 0 bytes is a *valid* estimate
//     (the topic is empty or fully purged). We return OK=true with Bytes=0
//     so the caller can flag PURGED / APPEARS_NEVER_USED elsewhere.
func EstimateTonnage(in EstimateInput) EstimateResult {
	if in.SegmentBytes <= 0 {
		return EstimateResult{OK: false, Reason: "no segment bytes reported by broker"}
	}
	if in.SegmentRecordCount <= 0 {
		return EstimateResult{OK: false, Reason: "no segment record count reported by broker"}
	}
	avg := float64(in.SegmentBytes) / float64(in.SegmentRecordCount)
	live := in.LatestOffsetSum - in.EarliestOffsetSum
	if live < 0 {
		// Should not happen; brokers always report start <= end. Treat as
		// UNKNOWN rather than producing a negative byte count.
		return EstimateResult{OK: false, Reason: "earliest_offset > latest_offset (broker inconsistency)"}
	}
	bytes := int64(float64(live) * avg)
	if bytes < 0 {
		// Overflow guard: a pathological avg × delta exceeded int64.
		return EstimateResult{OK: false, Reason: "estimate overflows int64"}
	}
	return EstimateResult{
		Bytes:          bytes,
		AvgRecordBytes: avg,
		OK:             true,
	}
}
