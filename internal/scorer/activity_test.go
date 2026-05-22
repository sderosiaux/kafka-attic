package scorer

import (
	"testing"
	"time"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

// defaultCurve is the SPEC §4.2 default activity curve. Re-declared as the
// types form so the test focuses on interpolation behavior rather than
// config-to-types plumbing.
var defaultCurve = []types.ActivityCurvePoint{
	{Days: 0, Score: 0},
	{Days: 30, Score: 25},
	{Days: 90, Score: 60},
	{Days: 180, Score: 80},
	{Days: 365, Score: 100},
}

func TestInterpolateCurve(t *testing.T) {
	cases := []struct {
		days float64
		want int
	}{
		{0, 0},
		{30, 25},
		{90, 60},
		{180, 80},
		{287, 92}, // SPEC §4.6 worked example: 80 + 107/185 × 20 ≈ 91.57 → 92
		{365, 100},
		{500, 100}, // beyond final anchor → clamped to last score
	}
	for _, c := range cases {
		got := interpolateCurve(c.days, defaultCurve)
		if got != c.want {
			t.Errorf("interpolateCurve(%v) = %d, want %d", c.days, got, c.want)
		}
	}
}

func TestScoreActivity_LogAppendTimeKnown(t *testing.T) {
	now := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	ts := now.AddDate(0, 0, -30)
	score, ev, days, ok := scoreActivity(now, &ts, "LogAppendTime", defaultCurve)
	if !ok {
		t.Fatalf("expected ok")
	}
	if score != 25 {
		t.Errorf("score=%d want 25", score)
	}
	if ev != types.EvidenceKnown {
		t.Errorf("evidence=%v want KNOWN", ev)
	}
	if days < 29.9 || days > 30.1 {
		t.Errorf("days=%v want ~30", days)
	}
}

func TestScoreActivity_CreateTimeEstimated(t *testing.T) {
	now := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	ts := now.AddDate(0, 0, -287)
	score, ev, _, ok := scoreActivity(now, &ts, "CreateTime", defaultCurve)
	if !ok {
		t.Fatalf("expected ok")
	}
	if score != 92 {
		t.Errorf("score=%d want 92 (287d interpolated)", score)
	}
	if ev != types.EvidenceEstimated {
		t.Errorf("evidence=%v want ESTIMATED", ev)
	}
}

func TestScoreActivity_NoTimestampUnknown(t *testing.T) {
	score, ev, _, ok := scoreActivity(time.Now(), nil, "CreateTime", defaultCurve)
	if ok {
		t.Fatalf("expected !ok on missing ts")
	}
	if score != neutralScore {
		t.Errorf("score=%d want %d (neutral)", score, neutralScore)
	}
	if ev != types.EvidenceUnknown {
		t.Errorf("evidence=%v want UNKNOWN", ev)
	}
}

func TestScoreActivity_DaysBoundaries(t *testing.T) {
	now := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		days int
		want int
	}{
		{0, 0},
		{30, 25},
		{90, 60},
		{180, 80},
		{287, 92},
		{365, 100},
		{500, 100},
	}
	for _, c := range cases {
		ts := now.AddDate(0, 0, -c.days)
		score, _, _, _ := scoreActivity(now, &ts, "LogAppendTime", defaultCurve)
		if score != c.want {
			t.Errorf("days=%d score=%d want %d", c.days, score, c.want)
		}
	}
}
