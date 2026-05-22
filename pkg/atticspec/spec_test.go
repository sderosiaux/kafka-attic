package atticspec

import "testing"

// TestSpecVersionPinned guards the published version string. A bump here
// is a deliberate, breaking-change-aware action.
func TestSpecVersionPinned(t *testing.T) {
	if SpecVersion != "1.0.0" {
		t.Fatalf("SpecVersion drifted: got %q, want %q", SpecVersion, "1.0.0")
	}
}

// TestDefaultWeightsSumToOne pins the default weights and verifies the
// sum invariant. Any change is intentionally a diff-visible event.
func TestDefaultWeightsSumToOne(t *testing.T) {
	w := DefaultWeights
	if w.Activity != 0.30 || w.Tenancy != 0.20 || w.Tonnage != 0.10 ||
		w.Intent != 0.15 || w.Consumption != 0.25 {
		t.Fatalf("default weights drifted: %+v", w)
	}
	sum := w.Activity + w.Tenancy + w.Tonnage + w.Intent + w.Consumption
	if sum < 0.9999 || sum > 1.0001 {
		t.Fatalf("default weights must sum to 1.0, got %f", sum)
	}
}

// TestDefaultThresholdsPinned guards the verdict bands.
func TestDefaultThresholdsPinned(t *testing.T) {
	want := Thresholds{LikelyUnused: 90, Candidate: 70, Inspect: 40}
	if DefaultThresholds != want {
		t.Fatalf("default thresholds drifted: got %+v, want %+v", DefaultThresholds, want)
	}
}

// TestDefaultActivityCurvePinned guards the piecewise-linear anchors.
func TestDefaultActivityCurvePinned(t *testing.T) {
	want := []ActivityCurvePoint{
		{Days: 0, Score: 0},
		{Days: 30, Score: 25},
		{Days: 90, Score: 60},
		{Days: 180, Score: 80},
		{Days: 365, Score: 100},
	}
	if len(DefaultActivityCurve) != len(want) {
		t.Fatalf("activity curve length drifted: got %d, want %d", len(DefaultActivityCurve), len(want))
	}
	for i, p := range want {
		if DefaultActivityCurve[i] != p {
			t.Fatalf("activity curve point %d drifted: got %+v, want %+v", i, DefaultActivityCurve[i], p)
		}
	}
}

// TestVerdictEnumStable pins the verdict enum strings used in JSON.
func TestVerdictEnumStable(t *testing.T) {
	cases := map[Verdict]string{
		VerdictLikelyUnused: "LIKELY_UNUSED",
		VerdictCandidate:    "CANDIDATE",
		VerdictInspect:      "INSPECT",
		VerdictActive:       "ACTIVE",
	}
	for v, want := range cases {
		if string(v) != want {
			t.Fatalf("verdict %v drifted: got %q, want %q", v, string(v), want)
		}
	}
}

// TestFlagEnumStable pins the flag enum strings used in JSON.
func TestFlagEnumStable(t *testing.T) {
	cases := map[Flag]string{
		FlagAppearsNeverUsed: "APPEARS_NEVER_USED",
		FlagPurged:           "PURGED",
		FlagOversized:        "OVERSIZED",
		FlagSkewed:           "SKEWED",
		FlagOrphanSchema:     "ORPHAN_SCHEMA",
		FlagCompacted:        "COMPACTED",
		FlagRemoteStorage:    "REMOTE_STORAGE",
		FlagMissingSignal:    "MISSING_SIGNAL",
	}
	for f, want := range cases {
		if string(f) != want {
			t.Fatalf("flag %v drifted: got %q, want %q", f, string(f), want)
		}
	}
}

// TestEvidenceEnumStable pins the evidence enum strings.
func TestEvidenceEnumStable(t *testing.T) {
	cases := map[Evidence]string{
		EvidenceKnown:     "KNOWN",
		EvidenceEstimated: "ESTIMATED",
		EvidenceUnknown:   "UNKNOWN",
	}
	for e, want := range cases {
		if string(e) != want {
			t.Fatalf("evidence %v drifted: got %q, want %q", e, string(e), want)
		}
	}
}

// TestSubSignalEnumStable pins the sub-signal enum strings.
func TestSubSignalEnumStable(t *testing.T) {
	cases := map[SubSignal]string{
		SubSignalActivity:    "activity",
		SubSignalTenancy:     "tenancy",
		SubSignalTonnage:     "tonnage",
		SubSignalIntent:      "intent",
		SubSignalConsumption: "consumption",
	}
	for s, want := range cases {
		if string(s) != want {
			t.Fatalf("sub-signal %v drifted: got %q, want %q", s, string(s), want)
		}
	}
}

// TestVerdictCapsPinned guards the cap rule set; adding a cap is a
// minor-version event that must show in the diff of this test.
func TestVerdictCapsPinned(t *testing.T) {
	want := []VerdictCap{
		{Reason: "MISSING_SIGNAL", MaxVerdict: VerdictInspect},
		{Reason: "ESTIMATED_EVIDENCE", MaxVerdict: VerdictCandidate},
		{Reason: "COMPACTED", MaxVerdict: VerdictInspect},
		{Reason: "REMOTE_STORAGE", MaxVerdict: VerdictInspect},
		{Reason: "APPEARS_NEVER_USED", MaxVerdict: VerdictCandidate},
	}
	if len(VerdictCaps) != len(want) {
		t.Fatalf("verdict caps length drifted: got %d, want %d", len(VerdictCaps), len(want))
	}
	for i, c := range want {
		if VerdictCaps[i] != c {
			t.Fatalf("cap %d drifted: got %+v, want %+v", i, VerdictCaps[i], c)
		}
	}
}

// TestSkippableSubSignalsPinned guards which sub-signals are skippable
// (weight redistribution) vs MISSING_SIGNAL on UNKNOWN evidence.
func TestSkippableSubSignalsPinned(t *testing.T) {
	want := []SubSignal{SubSignalTonnage, SubSignalIntent}
	if len(SkippableSubSignals) != len(want) {
		t.Fatalf("skippable sub-signals length drifted: got %d, want %d", len(SkippableSubSignals), len(want))
	}
	for i, s := range want {
		if SkippableSubSignals[i] != s {
			t.Fatalf("skippable sub-signal %d drifted: got %q, want %q", i, SkippableSubSignals[i], s)
		}
	}
}
