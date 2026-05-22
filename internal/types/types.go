// Package types contains the shared data model used across kafka-attic.
// All JSON tags follow the snake_case schema in SPEC Appendix C.
package types

import "time"

// Verdict is the machine enum for the overall topic verdict.
type Verdict string

const (
	VerdictLikelyUnused Verdict = "LIKELY_UNUSED"
	VerdictCandidate    Verdict = "CANDIDATE"
	VerdictInspect      Verdict = "INSPECT"
	VerdictActive       Verdict = "ACTIVE"
)

// Display returns the human label for a verdict (terminal/HTML).
func (v Verdict) Display() string {
	switch v {
	case VerdictLikelyUnused:
		return "Likely unused"
	case VerdictCandidate:
		return "Candidate"
	case VerdictInspect:
		return "Inspect"
	case VerdictActive:
		return "Active"
	default:
		return string(v)
	}
}

// Flag annotates a topic with a structured marker.
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

// Display returns the human label for a flag.
func (f Flag) Display() string {
	switch f {
	case FlagAppearsNeverUsed:
		return "Appears never used (low evidence)"
	case FlagPurged:
		return "Records purged by retention"
	case FlagOversized:
		return "Over-provisioned partitions"
	case FlagSkewed:
		return "Partition load uneven"
	case FlagOrphanSchema:
		return "No schema reference found"
	case FlagCompacted:
		return "Compacted topic; manual review required"
	case FlagRemoteStorage:
		return "Tiered storage; storage unknown"
	case FlagMissingSignal:
		return "Some signals unavailable"
	default:
		return string(f)
	}
}

// Evidence is the trust level for a collected sub-signal.
type Evidence string

const (
	EvidenceKnown     Evidence = "KNOWN"
	EvidenceEstimated Evidence = "ESTIMATED"
	EvidenceUnknown   Evidence = "UNKNOWN"
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

// SubScore is one of the five ATTIC components with its evidence + raw inputs.
type SubScore struct {
	Score    int            `json:"score"`
	Evidence Evidence       `json:"evidence"`
	Input    map[string]any `json:"input,omitempty"`
	// Skipped is true when the sub-signal was excluded from scoring entirely
	// (Tonnage/Intent only) and its weight was redistributed.
	Skipped bool `json:"skipped,omitempty"`
}

// AtticScore is the per-topic ATTIC result.
type AtticScore struct {
	SpecVersion    string                  `json:"spec_version"`
	SubScores      map[SubSignal]SubScore  `json:"sub_scores"`
	RawScore       float64                 `json:"raw_score"`
	Verdict        Verdict                 `json:"verdict"`
	VerdictCappedBy *string                `json:"verdict_capped_by"`
}

// StorageInfo describes a topic's storage footprint and how it was obtained.
type StorageInfo struct {
	// Bytes is null when truly unknown.
	Bytes    *int64   `json:"bytes"`
	Source   string   `json:"source"`   // log_dir | estimate | unknown
	Evidence Evidence `json:"evidence"`
}

// PartitionMetric is the per-partition snapshot.
type PartitionMetric struct {
	Partition      int32  `json:"partition"`
	EarliestOffset int64  `json:"earliest_offset"`
	LatestOffset   int64  `json:"latest_offset"`
	SizeBytes      *int64 `json:"size_bytes"`
	Leader         int32  `json:"leader"`
}

// ConsumerGroupInfo is one consumer group's view of a topic.
type ConsumerGroupInfo struct {
	GroupID           string `json:"group_id"`
	State             string `json:"state"`
	MemberCount       int    `json:"member_count"`
	CommittedOffsetSum int64 `json:"committed_offset_sum"`
	LagSum            int64  `json:"lag_sum"`
}

// SchemaRegistryInfo is the SR view for a topic.
type SchemaRegistryInfo struct {
	SubjectStrategy string   `json:"subject_strategy"`
	SubjectsFound   []string `json:"subjects_found"`
	Evidence        Evidence `json:"evidence"`
}

// OwnerInfo carries the resolved owner for a topic, when known.
type OwnerInfo struct {
	Value     string  `json:"value"`
	Source    string  `json:"source"`
	EntityRef *string `json:"entity_ref,omitempty"`
}

// Topic is the full per-topic snapshot row.
type Topic struct {
	Name                  string              `json:"name"`
	NameRedacted          *string             `json:"name_redacted"`
	Partitions            int                 `json:"partitions"`
	ReplicationFactor     int                 `json:"replication_factor"`
	CleanupPolicy         string              `json:"cleanup_policy"`
	RetentionMs           int64               `json:"retention_ms"`
	RemoteStorageEnabled  bool                `json:"remote_storage_enabled"`
	MessageTimestampType  string              `json:"message_timestamp_type"`
	LastProduceTs         *time.Time          `json:"last_produce_ts"`
	EarliestOffsetSum     int64               `json:"earliest_offset_sum"`
	LatestOffsetSum       int64               `json:"latest_offset_sum"`
	Storage               StorageInfo         `json:"storage"`
	PartitionMetrics      []PartitionMetric   `json:"partition_metrics"`
	ConsumerGroups        []ConsumerGroupInfo `json:"consumer_groups"`
	SchemaRegistry        *SchemaRegistryInfo `json:"schema_registry,omitempty"`
	Attic                 AtticScore          `json:"attic"`
	Flags                 []Flag              `json:"flags"`
	Owner                 *OwnerInfo          `json:"owner"`
	SignalsMissing        []SubSignal         `json:"signals_missing"`
	ExcludedByPattern     bool                `json:"excluded_by_pattern,omitempty"`
}

// ClusterInfo describes the connected cluster.
type ClusterInfo struct {
	Name                  string `json:"name"`
	Bootstrap             string `json:"bootstrap"`
	DetectedType          string `json:"detected_type"`
	KafkaVersionReported  string `json:"kafka_version_reported"`
}

// PermissionsObserved records which APIs succeeded during the scan.
type PermissionsObserved struct {
	DescribeCluster    bool `json:"describe_cluster"`
	DescribeTopics     bool `json:"describe_topics"`
	DescribeConfigs    bool `json:"describe_configs"`
	DescribeGroups     bool `json:"describe_groups"`
	DescribeLogDirs    bool `json:"describe_log_dirs"`
	SchemaRegistryRead bool `json:"schema_registry_read"`
}

// ConfigSnapshot captures the active scoring config in the snapshot.
type ConfigSnapshot struct {
	AtticWeights  AtticWeights         `json:"attic_weights"`
	Thresholds    AtticThresholds      `json:"thresholds"`
	ActivityCurve []ActivityCurvePoint `json:"activity_curve"`
}

// ScanInfo summarizes the scan run itself.
type ScanInfo struct {
	TopicCountScanned          int                 `json:"topic_count_scanned"`
	TopicCountExcludedByPattern int                `json:"topic_count_excluded_by_pattern"`
	DurationMs                 int64               `json:"duration_ms"`
	PermissionsObserved        PermissionsObserved `json:"permissions_observed"`
	MissingSignalsGlobal       []string            `json:"missing_signals_global"`
	ConfigSnapshot             ConfigSnapshot      `json:"config_snapshot"`
}

// TelemetryBlock is the optional telemetry envelope of a snapshot.
type TelemetryBlock struct {
	AnonymousRunUUID  string  `json:"anonymous_run_uuid"`
	SharedSummaryURL  *string `json:"shared_summary_url"`
}

// Snapshot is the top-level JSON snapshot consumed by `kattic diff`.
type Snapshot struct {
	SchemaVersion      string         `json:"schema_version"`
	AtticSpecVersion   string         `json:"attic_spec_version"`
	GeneratedAt        time.Time      `json:"generated_at"`
	KafkaAtticVersion  string         `json:"kafka_attic_version"`
	Cluster            ClusterInfo    `json:"cluster"`
	Scan               ScanInfo       `json:"scan"`
	Topics             []Topic        `json:"topics"`
	Telemetry          TelemetryBlock `json:"telemetry"`
}

// AtticWeights are the per-sub-signal weights; they must sum to 1.0.
type AtticWeights struct {
	Activity    float64 `json:"activity"`
	Tenancy     float64 `json:"tenancy"`
	Tonnage     float64 `json:"tonnage"`
	Intent      float64 `json:"intent"`
	Consumption float64 `json:"consumption"`
}

// AtticThresholds defines the verdict bands.
type AtticThresholds struct {
	LikelyUnused int `json:"likely_unused"`
	Candidate    int `json:"candidate"`
	Inspect      int `json:"inspect"`
}

// ActivityCurvePoint is one (days, score) anchor in the piecewise-linear curve.
type ActivityCurvePoint struct {
	Days  int `json:"days" yaml:"days"`
	Score int `json:"score" yaml:"score"`
}
