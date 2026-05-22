package renderer

import (
	"time"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

// fixedNow is the reference clock used by every renderer test to make
// "287d ago" / "2h ago" outputs deterministic.
var fixedNow = time.Date(2026, 5, 21, 9, 38, 0, 0, time.UTC)

func ptrInt64(v int64) *int64        { return &v }
func ptrTime(v time.Time) *time.Time { return &v }

// fixtureSnapshot returns the snapshot exercised by every renderer test.
// It covers each verdict band and every flag relevant to display.
func fixtureSnapshot() *types.Snapshot {
	weights := types.AtticWeights{
		Activity: 0.30, Tenancy: 0.20, Tonnage: 0.10, Intent: 0.15, Consumption: 0.25,
	}
	curve := []types.ActivityCurvePoint{
		{Days: 0, Score: 0},
		{Days: 30, Score: 25},
		{Days: 90, Score: 60},
		{Days: 180, Score: 80},
		{Days: 365, Score: 100},
	}

	mkSub := func() map[types.SubSignal]types.SubScore {
		return map[types.SubSignal]types.SubScore{
			types.SubSignalActivity:    {Score: 92, Evidence: types.EvidenceKnown},
			types.SubSignalTenancy:     {Score: 100, Evidence: types.EvidenceKnown},
			types.SubSignalTonnage:     {Score: 96, Evidence: types.EvidenceKnown},
			types.SubSignalIntent:      {Score: 100, Evidence: types.EvidenceKnown},
			types.SubSignalConsumption: {Score: 0, Evidence: types.EvidenceKnown},
		}
	}

	return &types.Snapshot{
		SchemaVersion:     "1.0.0",
		AtticSpecVersion:  "1.0.0",
		GeneratedAt:       time.Date(2026, 5, 21, 9, 38, 0, 0, time.UTC),
		KafkaAtticVersion: "1.0.0",
		Cluster: types.ClusterInfo{
			Name:                 "prod-msk",
			Bootstrap:            "b-1.msk.eu-west-1.amazonaws.com:9098",
			DetectedType:         "msk",
			KafkaVersionReported: "3.7.0",
		},
		Scan: types.ScanInfo{
			TopicCountScanned:           7,
			TopicCountExcludedByPattern: 0,
			DurationMs:                  18420,
			PermissionsObserved: types.PermissionsObserved{
				DescribeCluster: true, DescribeTopics: true, DescribeConfigs: true,
				DescribeGroups: true, DescribeLogDirs: true, SchemaRegistryRead: true,
			},
			MissingSignalsGlobal: []string{},
			ConfigSnapshot: types.ConfigSnapshot{
				AtticWeights:  weights,
				Thresholds:    types.AtticThresholds{LikelyUnused: 90, Candidate: 70, Inspect: 40},
				ActivityCurve: curve,
			},
		},
		Topics: []types.Topic{
			{
				Name:                 "legacy-events",
				Partitions:           6,
				ReplicationFactor:    3,
				CleanupPolicy:        "delete",
				RetentionMs:          604800000,
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        ptrTime(fixedNow.AddDate(0, 0, -287)),
				Storage: types.StorageInfo{
					Bytes:    ptrInt64(13207180800), // 13.2 GB → "13.2 GB" (close to spec sample's 12.3)
					Source:   "log_dir",
					Evidence: types.EvidenceKnown,
				},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0",
					SubScores:   mkSub(),
					RawScore:    72,
					Verdict:     types.VerdictCandidate,
				},
				Flags:          []types.Flag{types.FlagOrphanSchema},
				SignalsMissing: []types.SubSignal{},
			},
			{
				Name:                 "audit-trail",
				Partitions:           3,
				ReplicationFactor:    3,
				CleanupPolicy:        "delete",
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        ptrTime(fixedNow.Add(-3 * 24 * time.Hour)),
				Storage: types.StorageInfo{
					Bytes:    ptrInt64(890_000_000),
					Source:   "log_dir",
					Evidence: types.EvidenceKnown,
				},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0",
					SubScores:   mkSub(),
					RawScore:    12,
					Verdict:     types.VerdictActive,
				},
				Flags:          []types.Flag{},
				SignalsMissing: []types.SubSignal{},
			},
			{
				Name:                 "empty-topic",
				Partitions:           1,
				ReplicationFactor:    3,
				CleanupPolicy:        "delete",
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        nil,
				Storage: types.StorageInfo{
					Bytes:    ptrInt64(0),
					Source:   "log_dir",
					Evidence: types.EvidenceKnown,
				},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0",
					SubScores:   mkSub(),
					RawScore:    88,
					Verdict:     types.VerdictCandidate,
				},
				Flags:          []types.Flag{types.FlagAppearsNeverUsed},
				SignalsMissing: []types.SubSignal{},
			},
			{
				Name:                 "old-events",
				Partitions:           2,
				ReplicationFactor:    3,
				CleanupPolicy:        "delete",
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        ptrTime(fixedNow.Add(-180 * 24 * time.Hour)),
				Storage: types.StorageInfo{
					Bytes:    ptrInt64(0),
					Source:   "log_dir",
					Evidence: types.EvidenceKnown,
				},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0",
					SubScores:   mkSub(),
					RawScore:    95,
					Verdict:     types.VerdictLikelyUnused,
				},
				Flags:          []types.Flag{types.FlagPurged},
				SignalsMissing: []types.SubSignal{},
			},
			{
				Name:                 "oversized-events",
				Partitions:           32,
				ReplicationFactor:    3,
				CleanupPolicy:        "delete",
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        ptrTime(fixedNow.Add(-2 * time.Hour)),
				Storage: types.StorageInfo{
					Bytes:    ptrInt64(412_000_000_000),
					Source:   "log_dir",
					Evidence: types.EvidenceKnown,
				},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0",
					SubScores:   mkSub(),
					RawScore:    55,
					Verdict:     types.VerdictInspect,
				},
				Flags:          []types.Flag{types.FlagOversized, types.FlagSkewed},
				SignalsMissing: []types.SubSignal{},
			},
			{
				Name:                 "compacted-state",
				Partitions:           4,
				ReplicationFactor:    3,
				CleanupPolicy:        "compact",
				MessageTimestampType: "CreateTime",
				LastProduceTS:        ptrTime(fixedNow.Add(-1 * 24 * time.Hour)),
				Storage: types.StorageInfo{
					Bytes:    ptrInt64(5_200_000_000),
					Source:   "estimate",
					Evidence: types.EvidenceEstimated,
				},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0",
					SubScores:   mkSub(),
					RawScore:    42,
					Verdict:     types.VerdictInspect,
				},
				Flags:          []types.Flag{types.FlagCompacted},
				SignalsMissing: []types.SubSignal{},
			},
			{
				Name:                 "remote-archive",
				Partitions:           6,
				ReplicationFactor:    3,
				CleanupPolicy:        "delete",
				RemoteStorageEnabled: true,
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        ptrTime(fixedNow.Add(-90 * 24 * time.Hour)),
				Storage: types.StorageInfo{
					Bytes:    nil,
					Source:   "unknown",
					Evidence: types.EvidenceUnknown,
				},
				// SubScores intentionally empty so SCORE renders "—".
				Attic: types.AtticScore{
					SpecVersion: "1.0.0",
					SubScores:   map[types.SubSignal]types.SubScore{},
				},
				Flags:          []types.Flag{types.FlagRemoteStorage},
				SignalsMissing: []types.SubSignal{},
			},
		},
		Telemetry: types.TelemetryBlock{},
	}
}
