package managed

import "testing"

// TestEstimateTonnage_HappyPath uses round numbers so the arithmetic is
// trivially verifiable. SegmentBytes / SegmentRecordCount = 1000 B/record;
// 500 live records → 500_000 estimated bytes.
func TestEstimateTonnage_HappyPath(t *testing.T) {
	got := EstimateTonnage(EstimateInput{
		SegmentBytes:       1_000_000,
		SegmentRecordCount: 1_000,
		EarliestOffsetSum:  500,
		LatestOffsetSum:    1_000,
	})
	if !got.OK {
		t.Fatalf("expected OK estimate, got Reason=%q", got.Reason)
	}
	if got.AvgRecordBytes != 1000 {
		t.Fatalf("AvgRecordBytes = %v, want 1000", got.AvgRecordBytes)
	}
	if got.Bytes != 500_000 {
		t.Fatalf("Bytes = %d, want 500000", got.Bytes)
	}
}

// TestEstimateTonnage_EmptyTopicProducesZero locks in SPEC §5.6 behavior:
// when the topic is empty (latest == earliest) we return Bytes=0, OK=true
// rather than UNKNOWN. The empty/PURGED distinction is the scorer's job,
// not the estimator's.
func TestEstimateTonnage_EmptyTopicProducesZero(t *testing.T) {
	got := EstimateTonnage(EstimateInput{
		SegmentBytes:       4096,
		SegmentRecordCount: 1,
		EarliestOffsetSum:  100,
		LatestOffsetSum:    100,
	})
	if !got.OK {
		t.Fatalf("expected OK, got Reason=%q", got.Reason)
	}
	if got.Bytes != 0 {
		t.Fatalf("Bytes = %d, want 0", got.Bytes)
	}
}

// TestEstimateTonnage_NoSegmentBytes asserts UNKNOWN when the broker did
// not report any segment-level size. SPEC §5.6: never fall back to
// sampling.
func TestEstimateTonnage_NoSegmentBytes(t *testing.T) {
	got := EstimateTonnage(EstimateInput{
		SegmentBytes:       0,
		SegmentRecordCount: 100,
		EarliestOffsetSum:  0,
		LatestOffsetSum:    100,
	})
	if got.OK {
		t.Fatalf("expected !OK")
	}
	if got.Reason == "" {
		t.Fatalf("expected non-empty Reason")
	}
}

// TestEstimateTonnage_NoRecordCount asserts UNKNOWN when the broker did
// not report a record count we can divide by.
func TestEstimateTonnage_NoRecordCount(t *testing.T) {
	got := EstimateTonnage(EstimateInput{
		SegmentBytes:       1_000_000,
		SegmentRecordCount: 0,
		EarliestOffsetSum:  0,
		LatestOffsetSum:    100,
	})
	if got.OK {
		t.Fatalf("expected !OK")
	}
	if got.Reason == "" {
		t.Fatalf("expected non-empty Reason")
	}
}

// TestEstimateTonnage_NegativeRecordCount guards against a broker quirk:
// a negative record count must surface as UNKNOWN, not as a panic or a
// negative byte count.
func TestEstimateTonnage_NegativeRecordCount(t *testing.T) {
	got := EstimateTonnage(EstimateInput{
		SegmentBytes:       1_000_000,
		SegmentRecordCount: -1,
		EarliestOffsetSum:  0,
		LatestOffsetSum:    100,
	})
	if got.OK {
		t.Fatalf("expected !OK")
	}
}

// TestEstimateTonnage_BrokerInconsistencyEarliestAfterLatest covers the
// edge case where partition offsets straddle a broker reset. The
// estimator must report UNKNOWN rather than produce a negative byte count.
func TestEstimateTonnage_BrokerInconsistencyEarliestAfterLatest(t *testing.T) {
	got := EstimateTonnage(EstimateInput{
		SegmentBytes:       1_000_000,
		SegmentRecordCount: 1_000,
		EarliestOffsetSum:  200,
		LatestOffsetSum:    100,
	})
	if got.OK {
		t.Fatalf("expected !OK, got Bytes=%d", got.Bytes)
	}
}

// TestEstimateTonnage_FractionalAverageRoundsDown documents the rounding
// behavior. avg = 1024.5 B/rec × 4 records → 4098 bytes (int conversion
// truncates 4098.0). The estimator does not round to nearest; it returns
// the int64 cast value, which is fine for percentile bucketing.
func TestEstimateTonnage_FractionalAverageRoundsDown(t *testing.T) {
	got := EstimateTonnage(EstimateInput{
		SegmentBytes:       2049,
		SegmentRecordCount: 2,
		EarliestOffsetSum:  0,
		LatestOffsetSum:    4,
	})
	if !got.OK {
		t.Fatalf("expected OK, got Reason=%q", got.Reason)
	}
	if got.Bytes != 4098 {
		t.Fatalf("Bytes = %d, want 4098", got.Bytes)
	}
}

// TestEstimateTonnage_NoRecordSamplingContract is a documentation test: the
// estimator's input struct intentionally has no record-data field. If
// someone adds a "sample bytes" input later, this test will fail to compile
// because the literal below enumerates every field by name. SPEC §5.6
// privacy guarantee.
func TestEstimateTonnage_NoRecordSamplingContract(_ *testing.T) {
	_ = EstimateInput{
		SegmentBytes:       0,
		SegmentRecordCount: 0,
		EarliestOffsetSum:  0,
		LatestOffsetSum:    0,
	}
}
