package collector

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/managed"
	"github.com/conduktor/kafka-attic/internal/types"
)

// TestBuildTopic_EstimateTonnage_FromSegmentMetadata locks in the M6 wiring:
// when DescribeLogDirs is denied (logs.Auth=true) but the per-topic segment
// summary inputs are present AND cfg.Metrics is configured, buildTopic must
// call managed.EstimateTonnage and stamp the storage block with
// source="estimate" / evidence="ESTIMATED".
//
// We feed round numbers so the arithmetic is trivially verifiable:
//
//	SegmentBytes        = 1_000_000
//	SegmentRecordCount  =     1_000   → avg = 1_000 B/record
//	EarliestOffsetSum   =       100   (sum across partitions 0..1)
//	LatestOffsetSum     =       600   (sum across partitions 0..1)
//	live records        =       500
//	estimated bytes     =   500_000
func TestBuildTopic_EstimateTonnage_FromSegmentMetadata(t *testing.T) {
	const topic = "estimate-target"

	topicsRes := &listTopicsResult{
		inScope: []string{topic},
		metadata: kadm.TopicDetails{
			topic: kadm.TopicDetail{
				Topic: topic,
				Partitions: kadm.PartitionDetails{
					0: kadm.PartitionDetail{Topic: topic, Partition: 0, Replicas: []int32{1, 2, 3}},
					1: kadm.PartitionDetail{Topic: topic, Partition: 1, Replicas: []int32{1, 2, 3}},
				},
			},
		},
		configs: map[string]map[string]string{
			topic: {
				"cleanup.policy":         "delete",
				"message.timestamp.type": "CreateTime",
				"retention.ms":           "604800000",
			},
		},
	}

	offsRes := &offsetsResult{
		Partitions: map[string]map[int32]types.PartitionMetric{
			topic: {
				0: {Partition: 0, EarliestOffset: 50, LatestOffset: 300},
				1: {Partition: 1, EarliestOffset: 50, LatestOffset: 300},
			},
		},
		LastProduceTs: map[string]int64{topic: 0},
		PartitionAuth: map[string]bool{},
	}

	groupsRes := &groupsResult{
		PerTopic:     map[string][]types.ConsumerGroupInfo{},
		DescribeAuth: false,
		FetchAuth:    false,
	}

	// DescribeLogDirs was denied, BUT we have the SPEC §5.6 segment-summary
	// inputs (in production these would be plumbed in from a metrics layer;
	// here we set them directly to exercise the estimate path).
	logRes := &logDirsResult{
		BytesByTopic:              map[string]int64{},
		PartitionBytes:            map[string]map[int32]int64{},
		SegmentBytesByTopic:       map[string]int64{topic: 1_000_000},
		SegmentRecordCountByTopic: map[string]int64{topic: 1_000},
		Auth:                      true,
	}

	srRes := &srResult{Configured: false}

	cfg := &config.Config{
		Metrics: &config.MetricsConfig{Source: "prometheus"},
	}

	got := buildTopic(topic, topicsRes, offsRes, groupsRes, logRes, srRes, managed.ClusterSelfManaged, cfg)

	if got.Storage.Source != "estimate" {
		t.Fatalf("Storage.Source = %q, want %q", got.Storage.Source, "estimate")
	}
	if got.Storage.Evidence != types.EvidenceEstimated {
		t.Fatalf("Storage.Evidence = %q, want %q", got.Storage.Evidence, types.EvidenceEstimated)
	}
	if got.Storage.Bytes == nil {
		t.Fatalf("Storage.Bytes is nil, want non-nil")
	}
	if *got.Storage.Bytes != 500_000 {
		t.Fatalf("Storage.Bytes = %d, want 500000", *got.Storage.Bytes)
	}
}

// TestBuildTopic_EstimateTonnage_NotAttempted_WhenMetricsNotConfigured locks
// in the conservative gating from the task brief: even when segment-summary
// inputs are present in logRes, buildTopic must NOT call EstimateTonnage
// unless cfg.Metrics is configured. Without a metrics source we have no
// trustworthy avg-record-size pipeline, so the topic stays UNKNOWN and the
// scorer's skip+redistribute path (SPEC Appendix E) applies.
func TestBuildTopic_EstimateTonnage_NotAttempted_WhenMetricsNotConfigured(t *testing.T) {
	const topic = "estimate-target"

	topicsRes := &listTopicsResult{
		inScope: []string{topic},
		metadata: kadm.TopicDetails{
			topic: kadm.TopicDetail{
				Topic:      topic,
				Partitions: kadm.PartitionDetails{0: kadm.PartitionDetail{Topic: topic, Partition: 0, Replicas: []int32{1}}},
			},
		},
		configs: map[string]map[string]string{
			topic: {"cleanup.policy": "delete", "message.timestamp.type": "CreateTime", "retention.ms": "-1"},
		},
	}
	offsRes := &offsetsResult{
		Partitions: map[string]map[int32]types.PartitionMetric{
			topic: {0: {Partition: 0, EarliestOffset: 0, LatestOffset: 100}},
		},
		LastProduceTs: map[string]int64{topic: 0},
		PartitionAuth: map[string]bool{},
	}
	groupsRes := &groupsResult{PerTopic: map[string][]types.ConsumerGroupInfo{}}
	logRes := &logDirsResult{
		BytesByTopic:              map[string]int64{},
		PartitionBytes:            map[string]map[int32]int64{},
		SegmentBytesByTopic:       map[string]int64{topic: 1_000_000},
		SegmentRecordCountByTopic: map[string]int64{topic: 1_000},
		Auth:                      true,
	}
	srRes := &srResult{Configured: false}

	// cfg.Metrics is nil → estimate path disabled.
	cfg := &config.Config{}

	got := buildTopic(topic, topicsRes, offsRes, groupsRes, logRes, srRes, managed.ClusterConfluentCloud, cfg)

	if got.Storage.Source != "unknown" {
		t.Fatalf("Storage.Source = %q, want %q", got.Storage.Source, "unknown")
	}
	if got.Storage.Evidence != types.EvidenceUnknown {
		t.Fatalf("Storage.Evidence = %q, want %q", got.Storage.Evidence, types.EvidenceUnknown)
	}
	if got.Storage.Bytes != nil {
		t.Fatalf("Storage.Bytes = %v, want nil", *got.Storage.Bytes)
	}
}

// TestBuildTopic_LogDirKnown_BeatsEstimate guards the precedence rule: if
// DescribeLogDirs returned exact bytes, the estimate path must not overwrite
// them — exact > estimated in the SPEC §4.2 evidence hierarchy.
func TestBuildTopic_LogDirKnown_BeatsEstimate(t *testing.T) {
	const topic = "exact-wins"

	topicsRes := &listTopicsResult{
		inScope: []string{topic},
		metadata: kadm.TopicDetails{
			topic: kadm.TopicDetail{
				Topic:      topic,
				Partitions: kadm.PartitionDetails{0: kadm.PartitionDetail{Topic: topic, Partition: 0, Replicas: []int32{1}}},
			},
		},
		configs: map[string]map[string]string{
			topic: {"cleanup.policy": "delete", "message.timestamp.type": "CreateTime", "retention.ms": "-1"},
		},
	}
	offsRes := &offsetsResult{
		Partitions: map[string]map[int32]types.PartitionMetric{
			topic: {0: {Partition: 0, EarliestOffset: 0, LatestOffset: 10}},
		},
		LastProduceTs: map[string]int64{topic: 0},
		PartitionAuth: map[string]bool{},
	}
	groupsRes := &groupsResult{PerTopic: map[string][]types.ConsumerGroupInfo{}}
	logRes := &logDirsResult{
		BytesByTopic:              map[string]int64{topic: 42_424_242},
		PartitionBytes:            map[string]map[int32]int64{topic: {0: 42_424_242}},
		SegmentBytesByTopic:       map[string]int64{topic: 1_000_000},
		SegmentRecordCountByTopic: map[string]int64{topic: 1_000},
		Auth:                      false,
	}
	srRes := &srResult{Configured: false}
	cfg := &config.Config{Metrics: &config.MetricsConfig{Source: "prometheus"}}

	got := buildTopic(topic, topicsRes, offsRes, groupsRes, logRes, srRes, managed.ClusterSelfManaged, cfg)

	if got.Storage.Source != "log_dir" {
		t.Fatalf("Storage.Source = %q, want %q", got.Storage.Source, "log_dir")
	}
	if got.Storage.Evidence != types.EvidenceKnown {
		t.Fatalf("Storage.Evidence = %q, want %q", got.Storage.Evidence, types.EvidenceKnown)
	}
	if got.Storage.Bytes == nil || *got.Storage.Bytes != 42_424_242 {
		t.Fatalf("Storage.Bytes = %v, want 42424242", got.Storage.Bytes)
	}
}
