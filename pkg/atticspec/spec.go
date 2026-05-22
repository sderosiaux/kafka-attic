package atticspec

// SpecVersion is the semver string of the ATTIC Score methodology defined
// by this package. It is the value implementations should embed in their
// JSON snapshots under attic_spec_version.
const SpecVersion = "1.0.0"

// Weights describes the per-sub-signal weights. Must sum to 1.0.
type Weights struct {
	Activity    float64 `json:"activity" yaml:"activity"`
	Tenancy     float64 `json:"tenancy" yaml:"tenancy"`
	Tonnage     float64 `json:"tonnage" yaml:"tonnage"`
	Intent      float64 `json:"intent" yaml:"intent"`
	Consumption float64 `json:"consumption" yaml:"consumption"`
}

// Thresholds defines the verdict band lower bounds.
type Thresholds struct {
	LikelyUnused int `json:"likely_unused" yaml:"likely_unused"`
	Candidate    int `json:"candidate" yaml:"candidate"`
	Inspect      int `json:"inspect" yaml:"inspect"`
}

// ActivityCurvePoint is one (days, score) anchor in the piecewise-linear
// Activity sub-signal curve.
type ActivityCurvePoint struct {
	Days  int `json:"days" yaml:"days"`
	Score int `json:"score" yaml:"score"`
}

// DefaultWeights are the spec-default per-sub-signal weights.
var DefaultWeights = Weights{
	Activity:    0.30,
	Tenancy:     0.20,
	Tonnage:     0.10,
	Intent:      0.15,
	Consumption: 0.25,
}

// DefaultThresholds are the spec-default verdict band lower bounds.
var DefaultThresholds = Thresholds{
	LikelyUnused: 90,
	Candidate:    70,
	Inspect:      40,
}

// DefaultActivityCurve is the spec-default Activity sub-signal curve.
var DefaultActivityCurve = []ActivityCurvePoint{
	{Days: 0, Score: 0},
	{Days: 30, Score: 25},
	{Days: 90, Score: 60},
	{Days: 180, Score: 80},
	{Days: 365, Score: 100},
}

// Verdict is the overall topic verdict enum.
type Verdict string

const (
	VerdictLikelyUnused Verdict = "LIKELY_UNUSED"
	VerdictCandidate    Verdict = "CANDIDATE"
	VerdictInspect      Verdict = "INSPECT"
	VerdictActive       Verdict = "ACTIVE"
)

// SubSignal names one of the five ATTIC sub-signals.
type SubSignal string

const (
	SubSignalActivity    SubSignal = "activity"
	SubSignalTenancy     SubSignal = "tenancy"
	SubSignalTonnage     SubSignal = "tonnage"
	SubSignalIntent      SubSignal = "intent"
	SubSignalConsumption SubSignal = "consumption"
)

// Evidence is the trust level for a collected sub-signal.
type Evidence string

const (
	EvidenceKnown     Evidence = "KNOWN"
	EvidenceEstimated Evidence = "ESTIMATED"
	EvidenceUnknown   Evidence = "UNKNOWN"
)

// Flag annotates a topic with a structured marker. Flags never lower the
// numeric score; several cap the verdict (see VerdictCaps).
type Flag string

const (
	FlagAppearsNeverUsed Flag = "APPEARS_NEVER_USED"
	FlagPurged           Flag = "PURGED"
	FlagOversized        Flag = "OVERSIZED"
	FlagSkewed           Flag = "SKEWED"
	FlagOrphanSchema     Flag = "ORPHAN_SCHEMA"
	FlagCompacted        Flag = "COMPACTED"
	FlagRemoteStorage    Flag = "REMOTE_STORAGE"
	FlagMissingSignal    Flag = "MISSING_SIGNAL"
)

// VerdictCap describes one verdict-capping condition from spec section 4.4.
type VerdictCap struct {
	// Reason is the machine-stable cap identifier embedded in
	// snapshots under attic.verdict_capped_by.
	Reason string
	// MaxVerdict is the highest verdict permitted when this cap fires.
	MaxVerdict Verdict
}

// VerdictCaps lists the verdict-capping rules in evaluation order.
// Implementations should apply caps after computing the raw band, and
// keep the strictest cap that fires.
var VerdictCaps = []VerdictCap{
	{Reason: "MISSING_SIGNAL", MaxVerdict: VerdictInspect},
	{Reason: "ESTIMATED_EVIDENCE", MaxVerdict: VerdictCandidate},
	{Reason: "COMPACTED", MaxVerdict: VerdictInspect},
	{Reason: "REMOTE_STORAGE", MaxVerdict: VerdictInspect},
	{Reason: "APPEARS_NEVER_USED", MaxVerdict: VerdictCandidate},
}

// SkippableSubSignals lists the sub-signals whose UNKNOWN evidence
// triggers weight redistribution rather than a MISSING_SIGNAL flag.
var SkippableSubSignals = []SubSignal{
	SubSignalTonnage,
	SubSignalIntent,
}
