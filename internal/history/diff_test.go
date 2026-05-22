package history

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/conduktor/kafka-attic/internal/types"
)

func TestDiffFourCategories(t *testing.T) {
	a := sampleSnapshot(
		time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		// Stays the same → ignored.
		topic("stable-active", types.VerdictActive, 10, mkBytes(50)),
		// Progresses to LIKELY_UNUSED → newly-likely-unused.
		topic("ageing", types.VerdictCandidate, 80, mkBytes(2000)),
		// Regresses (LIKELY_UNUSED → ACTIVE).
		topic("revived", types.VerdictLikelyUnused, 95, mkBytes(500)),
		// Deleted in B, known bytes → reclaimed.
		topic("removed-known", types.VerdictLikelyUnused, 96, mkBytes(10_000)),
		// Deleted in B, unknown bytes → not counted in reclaim.
		topic("removed-unknown", types.VerdictCandidate, 88, nil),
	)
	b := sampleSnapshot(
		time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC),
		topic("stable-active", types.VerdictActive, 11, mkBytes(60)),
		topic("ageing", types.VerdictLikelyUnused, 92, mkBytes(2000)),
		topic("revived", types.VerdictActive, 15, mkBytes(700)),
		// Brand new topic, already at LIKELY_UNUSED → newly-likely-unused.
		topic("greenfield-dead", types.VerdictLikelyUnused, 91, mkBytes(0)),
		// Brand new active topic → ignored.
		topic("greenfield-active", types.VerdictActive, 5, mkBytes(100)),
	)

	r := Diff(a, b)

	// 1. NewlyLikelyUnused: ageing (progressed) + greenfield-dead (new).
	want := []string{"ageing", "greenfield-dead"}
	if got := names(r.NewlyLikelyUnused); !equalStrings(got, want) {
		t.Errorf("NewlyLikelyUnused: got %v, want %v", got, want)
	}

	// 2. Regressions: revived only.
	if got := names(r.Regressions); !equalStrings(got, []string{"revived"}) {
		t.Errorf("Regressions: got %v, want [revived]", got)
	}
	if rev := r.Regressions[0]; rev.BeforeVerdict != types.VerdictLikelyUnused || rev.AfterVerdict != types.VerdictActive {
		t.Errorf("Regression verdicts: %+v", rev)
	}

	// 3. Deletions: removed-known + removed-unknown.
	if got := names(r.Deletions); !equalStrings(got, []string{"removed-known", "removed-unknown"}) {
		t.Errorf("Deletions: got %v", got)
	}

	// 4. Reclaimed bytes: only removed-known (10_000), removed-unknown listed separately.
	if r.ReclaimedBytes != 10_000 {
		t.Errorf("ReclaimedBytes: got %d, want 10000", r.ReclaimedBytes)
	}
	if !equalStrings(r.ReclaimedUnknownTopics, []string{"removed-unknown"}) {
		t.Errorf("ReclaimedUnknownTopics: got %v", r.ReclaimedUnknownTopics)
	}
}

func TestDiffEmpty(t *testing.T) {
	r := Diff(nil, nil)
	if r == nil {
		t.Fatal("Diff(nil, nil) should return a zero report, not nil")
	}
	if len(r.NewlyLikelyUnused)+len(r.Regressions)+len(r.Deletions) != 0 {
		t.Errorf("expected empty diff, got %+v", r)
	}
	if r.ReclaimedBytes != 0 {
		t.Errorf("expected 0 reclaimed bytes, got %d", r.ReclaimedBytes)
	}
}

func TestDiffNoChanges(t *testing.T) {
	a := sampleSnapshot(time.Now().UTC(),
		topic("x", types.VerdictActive, 5, mkBytes(100)),
		topic("y", types.VerdictLikelyUnused, 95, mkBytes(1)),
	)
	b := sampleSnapshot(time.Now().UTC(),
		topic("x", types.VerdictActive, 5, mkBytes(100)),
		topic("y", types.VerdictLikelyUnused, 95, mkBytes(1)),
	)
	r := Diff(a, b)
	if n := len(r.NewlyLikelyUnused) + len(r.Regressions) + len(r.Deletions); n != 0 {
		t.Errorf("expected stable diff, got %d entries", n)
	}
}

func TestDiffRenderHumanIncludesKeyFields(t *testing.T) {
	a := sampleSnapshot(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		topic("dead", types.VerdictCandidate, 80, mkBytes(1<<20)),
	)
	b := sampleSnapshot(time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC))
	r := Diff(a, b)

	var buf bytes.Buffer
	if err := r.RenderHuman(&buf); err != nil {
		t.Fatalf("RenderHuman: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"Deletions (1)", "dead", "Reclaimed bytes:"} {
		if !strings.Contains(out, want) {
			t.Errorf("human output missing %q:\n%s", want, out)
		}
	}
}

func TestDiffRenderJSONValid(t *testing.T) {
	a := sampleSnapshot(time.Now().UTC(), topic("p", types.VerdictLikelyUnused, 95, mkBytes(1)))
	b := sampleSnapshot(time.Now().UTC())
	r := Diff(a, b)

	var buf bytes.Buffer
	if err := r.RenderJSON(&buf); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	if !strings.Contains(buf.String(), `"reclaimed_bytes": 1`) {
		t.Errorf("JSON output missing reclaimed_bytes: %s", buf.String())
	}
}

func names(d []TopicDelta) []string {
	out := make([]string, len(d))
	for i, x := range d {
		out[i] = x.Name
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
