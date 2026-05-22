package types

// AtticSpecVersion is the frozen version of the ATTIC scoring methodology
// described in SPEC §4. It is recorded in every JSON snapshot.
const AtticSpecVersion = "1.0.0"

// DefaultAtticWeights are the default weights from SPEC §4.1. They must sum to 1.0.
var DefaultAtticWeights = AtticWeights{
	Activity:    0.30,
	Tenancy:     0.20,
	Tonnage:     0.10,
	Intent:      0.15,
	Consumption: 0.25,
}

// DefaultAtticThresholds are the default verdict bands from SPEC §4.3.
var DefaultAtticThresholds = AtticThresholds{
	LikelyUnused: 90,
	Candidate:    70,
	Inspect:      40,
}

// DefaultActivityCurve is the default Activity sub-signal curve from SPEC §4.2.
var DefaultActivityCurve = []ActivityCurvePoint{
	{Days: 0, Score: 0},
	{Days: 30, Score: 25},
	{Days: 90, Score: 60},
	{Days: 180, Score: 80},
	{Days: 365, Score: 100},
}
