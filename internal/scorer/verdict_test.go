package scorer

import (
	"math"
	"testing"
	"time"

	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/types"
)

func defaultThresholds() config.Thresholds {
	return config.Thresholds{LikelyUnused: 90, Candidate: 70, Inspect: 40}
}

func TestScoreToVerdictBands(t *testing.T) {
	th := defaultThresholds()
	cases := []struct {
		raw  float64
		want types.Verdict
	}{
		{0, types.VerdictActive},
		{39.9, types.VerdictActive},
		{40, types.VerdictInspect},
		{69.9, types.VerdictInspect},
		{70, types.VerdictCandidate},
		{89.9, types.VerdictCandidate},
		{90, types.VerdictLikelyUnused},
		{100, types.VerdictLikelyUnused},
	}
	for _, c := range cases {
		if got := scoreToVerdict(c.raw, th); got != c.want {
			t.Errorf("raw=%v got=%v want=%v", c.raw, got, c.want)
		}
	}
}

func TestApplyVerdictCaps_MissingSignal(t *testing.T) {
	out, by := applyVerdictCaps(types.VerdictLikelyUnused, true, false, []types.Flag{types.FlagMissingSignal}, false)
	if out != types.VerdictInspect || by != cappedByMissingSignal {
		t.Errorf("got %v %s want INSPECT MISSING_SIGNAL", out, by)
	}
}

func TestApplyVerdictCaps_EstimatedEvidence(t *testing.T) {
	out, by := applyVerdictCaps(types.VerdictLikelyUnused, false, true, nil, false)
	if out != types.VerdictCandidate || by != cappedByEstimatedEvidence {
		t.Errorf("got %v %s want CANDIDATE ESTIMATED_EVIDENCE", out, by)
	}
}

func TestApplyVerdictCaps_Compacted(t *testing.T) {
	out, by := applyVerdictCaps(types.VerdictLikelyUnused, false, false, []types.Flag{types.FlagCompacted}, false)
	if out != types.VerdictInspect || by != cappedByCompacted {
		t.Errorf("got %v %s want INSPECT COMPACTED", out, by)
	}
}

func TestApplyVerdictCaps_RemoteStorage(t *testing.T) {
	out, by := applyVerdictCaps(types.VerdictLikelyUnused, false, false, []types.Flag{types.FlagRemoteStorage}, false)
	if out != types.VerdictInspect || by != cappedByRemoteStorage {
		t.Errorf("got %v %s want INSPECT REMOTE_STORAGE", out, by)
	}
}

func TestApplyVerdictCaps_AppearsNeverUsedWithoutPurged(t *testing.T) {
	out, by := applyVerdictCaps(types.VerdictLikelyUnused, false, false, []types.Flag{types.FlagAppearsNeverUsed}, false)
	if out != types.VerdictCandidate || by != cappedByAppearsNeverUsed {
		t.Errorf("got %v %s want CANDIDATE APPEARS_NEVER_USED", out, by)
	}
}

func TestApplyVerdictCaps_AppearsNeverUsedWithPurged_NoCap(t *testing.T) {
	out, by := applyVerdictCaps(types.VerdictLikelyUnused, false, false, []types.Flag{types.FlagAppearsNeverUsed, types.FlagPurged}, true)
	if out != types.VerdictLikelyUnused || by != "" {
		t.Errorf("got %v %s want LIKELY_UNUSED no-cap", out, by)
	}
}

// SPEC §4.6 worked example reproduction. This test MUST pass.
func TestScore_SpecWorkedExample_LegacyEvents(t *testing.T) {
	cfg := &config.Config{
		AtticScore: config.AtticScoreConfig{
			SpecVersion: types.AtticSpecVersion,
			Weights: config.Weights{
				Activity: 0.30, Tenancy: 0.20, Tonnage: 0.10, Intent: 0.15, Consumption: 0.25,
			},
			Thresholds: config.Thresholds{LikelyUnused: 90, Candidate: 70, Inspect: 40},
			ActivityCurve: []config.ActivityCurvePoint{
				{Days: 0, Score: 0},
				{Days: 30, Score: 25},
				{Days: 90, Score: 60},
				{Days: 180, Score: 80},
				{Days: 365, Score: 100},
			},
		},
		SchemaRegistry: &config.SchemaRegistryConfig{
			Provider:        "confluent",
			URL:             "https://sr.local",
			SubjectStrategy: "topic_name",
		},
	}

	// 287 days ago, LogAppendTime.
	now := time.Date(2026, 5, 21, 9, 38, 0, 0, time.UTC)
	produced := now.AddDate(0, 0, -287)

	// Cluster size distribution placing legacy-events at p4. Use 25 sizes,
	// our topic strictly between sizes[0] and sizes[1] → percentile 4.
	clusterTopics := make([]types.Topic, 0, 26)
	for i := 0; i < 25; i++ {
		b := int64(i+1) * 10_000_000_000 // 10 GB increments
		clusterTopics = append(clusterTopics, types.Topic{
			Name:    "filler",
			Storage: types.StorageInfo{Bytes: &b, Evidence: types.EvidenceKnown},
		})
	}
	legacyBytes := int64(12_300_000_000) // 12.3 GB
	legacy := types.Topic{
		Name:                 "legacy-events",
		Partitions:           6,
		ReplicationFactor:    3,
		CleanupPolicy:        "delete",
		MessageTimestampType: "LogAppendTime",
		LastProduceTs:        &produced,
		EarliestOffsetSum:    145203,
		LatestOffsetSum:      12847291,
		Storage:              types.StorageInfo{Bytes: &legacyBytes, Source: "log_dir", Evidence: types.EvidenceKnown},
		PartitionMetrics: []types.PartitionMetric{
			{Partition: 0, EarliestOffset: 24180, LatestOffset: 2141215},
			{Partition: 1, EarliestOffset: 24216, LatestOffset: 2141050},
			{Partition: 2, EarliestOffset: 24300, LatestOffset: 2141010},
			{Partition: 3, EarliestOffset: 24500, LatestOffset: 2141000},
			{Partition: 4, EarliestOffset: 24007, LatestOffset: 2141000},
			{Partition: 5, EarliestOffset: 24000, LatestOffset: 2142016},
		},
		ConsumerGroups: []types.ConsumerGroupInfo{
			{GroupID: "ingest-v1", State: "Dead", MemberCount: 0, CommittedOffsetSum: 12847291, LagSum: 0},
		},
		SchemaRegistry: &types.SchemaRegistryInfo{
			SubjectStrategy: "topic_name",
			SubjectsFound:   []string{},
			Evidence:        types.EvidenceKnown,
		},
		SignalsMissing: []types.SubSignal{},
	}
	clusterTopics = append(clusterTopics, legacy)

	snap := &types.Snapshot{
		Topics: clusterTopics,
	}

	s := New(cfg, snap, now)
	target := &snap.Topics[len(snap.Topics)-1]
	s.Score(snap, target)

	if math.Abs(target.Attic.RawScore-72.2) > 0.05 {
		t.Errorf("raw_score=%v want 72.2 ±0.05", target.Attic.RawScore)
	}
	if target.Attic.Verdict != types.VerdictCandidate {
		t.Errorf("verdict=%v want CANDIDATE", target.Attic.Verdict)
	}

	// Spot-check per-signal contributions per SPEC §4.6:
	subs := target.Attic.SubScores
	if subs[types.SubSignalActivity].Score != 92 {
		t.Errorf("activity=%d want 92", subs[types.SubSignalActivity].Score)
	}
	if subs[types.SubSignalTenancy].Score != 100 {
		t.Errorf("tenancy=%d want 100", subs[types.SubSignalTenancy].Score)
	}
	if subs[types.SubSignalTonnage].Score != 96 {
		t.Errorf("tonnage=%d want 96", subs[types.SubSignalTonnage].Score)
	}
	if subs[types.SubSignalIntent].Score != 100 {
		t.Errorf("intent=%d want 100", subs[types.SubSignalIntent].Score)
	}
	if subs[types.SubSignalConsumption].Score != 0 {
		t.Errorf("consumption=%d want 0", subs[types.SubSignalConsumption].Score)
	}

	// ORPHAN_SCHEMA flag must be present.
	if !contains(target.Flags, types.FlagOrphanSchema) {
		t.Errorf("missing ORPHAN_SCHEMA in %v", target.Flags)
	}
}
