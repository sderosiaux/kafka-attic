// Package collector is the M1 data-collection layer. It turns a connected
// kadm.Client + kattic.yaml into a *types.Snapshot whose []Topic entries are
// fully populated with metadata, offsets, log-dir sizes, consumer-group
// state, and schema-registry references — but without the ATTIC scoring
// itself (that's M2).
//
// The package is structured so that each Kafka API touched lives in its own
// file (topics.go, offsets.go, groups.go, logdirs.go, schema_registry.go) and
// each one can fail independently. Auth failures degrade evidence rather
// than aborting the scan, matching SPEC §5.3.
package collector

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/conduktor/kafka-attic/internal/cluster"
	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/managed"
	"github.com/conduktor/kafka-attic/internal/types"
)

// Version constants the snapshot reports. Kept here so they are owned by the
// collector layer rather than the CLI shim.
const (
	SnapshotSchemaVersion = "1.0.0"
)

// Collect is the top-level entry point. It uses the provided cluster.Clients
// and config to populate a *types.Snapshot ready for the scorer (M2).
//
// The function never returns a partial snapshot: either it returns (snap,
// nil) with degraded evidence on individual signals, or (nil, err) when the
// scan cannot proceed (e.g. no metadata access at all).
func Collect(ctx context.Context, clients *cluster.Clients, cfg *config.Config) (*types.Snapshot, error) {
	if clients == nil || clients.Kadm == nil {
		// Defensive: callers should have failed earlier.
		return nil, errEmptyClient
	}
	return collectWith(ctx, adminFromClients(clients), nil, cfg)
}

// collectWith is the test-injectable entrypoint. NewSRClient is called here
// when srClient is nil and cfg has a schema_registry block, so tests can pass
// their own fake SR.
func collectWith(ctx context.Context, adm KafkaAdmin, srClient SRClient, cfg *config.Config) (*types.Snapshot, error) {
	if cfg == nil {
		return nil, errNilConfig
	}
	start := time.Now()

	// ── 1. Topics + configs ──────────────────────────────────────────────
	topicsRes, err := listTopicsAndConfigs(ctx, adm, cfg)
	if err != nil {
		return nil, err
	}

	inScopeSet := make(map[string]struct{}, len(topicsRes.inScope))
	for _, t := range topicsRes.inScope {
		inScopeSet[t] = struct{}{}
	}

	perms := types.PermissionsObserved{
		DescribeCluster: true,
		DescribeTopics:  true,
		DescribeConfigs: !topicsRes.configsAuthErr,
	}

	// ── 2. Offsets per partition (start/end + max ts) ────────────────────
	offsRes, err := listOffsets(ctx, adm, topicsRes.inScope)
	if err != nil {
		return nil, err
	}
	attachLeaders(offsRes, topicsRes.metadata)

	// ── 3. Consumer groups + committed offsets ───────────────────────────
	latest := latestPerPartitionFromMetrics(offsRes)
	groupsRes, err := listAndDescribeGroups(ctx, adm, inScopeSet, latest)
	if err != nil {
		return nil, err
	}
	perms.DescribeGroups = !groupsRes.DescribeAuth

	// ── 4. Log directories (storage bytes) ───────────────────────────────
	logRes, err := describeLogDirs(ctx, adm, offsRes)
	if err != nil {
		return nil, err
	}
	perms.DescribeLogDirs = !logRes.Auth

	// ── 4b. Detect cluster type (SPEC §5.5) ──────────────────────────────
	// We do this after log-dirs so the unauthorized signal feeds the
	// MSK Serverless / Confluent Cloud heuristics.
	anyTiered := false
	for _, cfgs := range topicsRes.configs {
		if managed.IsTieredStorage(managed.TieredCheck{Configs: cfgs, ClusterType: managed.ClusterConfluentCloud}) != managed.TieredNone {
			anyTiered = true
			break
		}
	}
	clusterType := managed.Detect(managed.DetectInput{
		Bootstrap:                   cfg.Cluster.Bootstrap,
		DescribeLogDirsUnauthorized: logRes.Auth,
		AnyTopicHasTieredConfig:     anyTiered,
	})

	// ── 5. Schema Registry (optional) ────────────────────────────────────
	if srClient == nil && cfg.SchemaRegistry != nil {
		built, sErr := NewSRClient(cfg)
		if sErr == nil {
			srClient = built
		}
	}
	srRes := collectSchemaRegistry(ctx, srClient, cfg, topicsRes.inScope)
	if srRes.Configured {
		perms.SchemaRegistryRead = srRes.Reachable
	}

	// ── 6. Assemble per-topic snapshots ──────────────────────────────────
	topics := make([]types.Topic, 0, len(topicsRes.inScope))
	for _, name := range topicsRes.inScope {
		t := buildTopic(name, topicsRes, offsRes, groupsRes, logRes, srRes, clusterType, cfg)
		topics = append(topics, t)
	}

	// Stable ordering by topic name so diff is deterministic.
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })

	snap := &types.Snapshot{
		SchemaVersion:    SnapshotSchemaVersion,
		AtticSpecVersion: types.AtticSpecVersion,
		GeneratedAt:      time.Now().UTC(),
		Cluster: types.ClusterInfo{
			Name:         cfg.Cluster.Name,
			Bootstrap:    cfg.Cluster.Bootstrap,
			DetectedType: string(clusterType),
		},
		Topics: topics,
	}
	snap.Scan = types.ScanInfo{
		TopicCountScanned:           len(topics),
		TopicCountExcludedByPattern: topicsRes.excludedCount,
		DurationMs:                  time.Since(start).Milliseconds(),
		PermissionsObserved:         perms,
		MissingSignalsGlobal:        buildGlobalMissing(perms, srRes, topicsRes.warnings),
		ConfigSnapshot:              configSnapshotFrom(cfg),
	}
	return snap, nil
}

// buildTopic assembles a single types.Topic from the four collectors' output.
// Per-topic flags from M2 (verdict caps, ORPHAN_SCHEMA…) are NOT computed
// here; this function only emits structural flags (REMOTE_STORAGE, COMPACTED,
// MISSING_SIGNAL) so the snapshot is well-formed without the scorer.
func buildTopic(
	name string,
	topicsRes *listTopicsResult,
	offs *offsetsResult,
	groups *groupsResult,
	logs *logDirsResult,
	sr *srResult,
	clusterType managed.ClusterType,
	cfg *config.Config,
) types.Topic {
	tdetail := topicsRes.metadata[name]
	cfgs := topicsRes.configs[name]

	// Partition metrics ── pull from offsets + log-dir.
	parts := offs.Partitions[name]
	pms := make([]types.PartitionMetric, 0, len(parts))
	var earliestSum, latestSum int64
	for _, pm := range parts {
		if logs != nil && logs.PartitionBytes[name] != nil {
			if sz, ok := logs.PartitionBytes[name][pm.Partition]; ok {
				v := sz
				pm.SizeBytes = &v
			}
		}
		pms = append(pms, pm)
		earliestSum += pm.EarliestOffset
		latestSum += pm.LatestOffset
	}
	sort.Slice(pms, func(i, j int) bool { return pms[i].Partition < pms[j].Partition })

	// Storage block — per SPEC §4.2 Tonnage:
	//   KNOWN     if DescribeLogDirs gave us bytes
	//   ESTIMATED if the SPEC §5.6 inputs (segment bytes + record count) are
	//             present for this topic and a metrics source is configured,
	//             AND DescribeLogDirs did not already give us an exact figure
	//   UNKNOWN   otherwise (Confluent Cloud / MSK Serverless typical case)
	//
	// The estimate path is gated on cfg.Metrics being configured because the
	// Kafka admin protocol (DescribeLogDirs through KIP-405 / KIP-848) does
	// not expose a segment-level record count: the avg-record-size figure
	// has to come from broker JMX metrics (or a vendor summary endpoint),
	// not from a kadm/kmsg response. When cfg.Metrics is nil we keep the
	// topic at storage.source="unknown" so the scorer skips Tonnage and
	// redistributes the weight per SPEC Appendix E.
	storage := types.StorageInfo{Source: "unknown", Evidence: types.EvidenceUnknown}
	hasLogDir := false
	if !logs.Auth {
		if bytes, ok := logs.BytesByTopic[name]; ok {
			b := bytes
			storage = types.StorageInfo{Bytes: &b, Source: "log_dir", Evidence: types.EvidenceKnown}
			hasLogDir = true
		}
	}
	if !hasLogDir && cfg != nil && cfg.Metrics != nil {
		segBytes, hasSegBytes := logs.SegmentBytesByTopic[name]
		segRecs, hasSegRecs := logs.SegmentRecordCountByTopic[name]
		// Only attempt the estimate when we have BOTH per-topic segment
		// inputs AND per-partition offsets (no offsets → no live-record
		// count to multiply by avg-record-size).
		_, hasParts := offs.Partitions[name]
		if hasSegBytes && hasSegRecs && hasParts {
			est := managed.EstimateTonnage(managed.EstimateInput{
				SegmentBytes:       segBytes,
				SegmentRecordCount: segRecs,
				EarliestOffsetSum:  earliestSum,
				LatestOffsetSum:    latestSum,
			})
			if est.OK {
				b := est.Bytes
				storage = types.StorageInfo{Bytes: &b, Source: "estimate", Evidence: types.EvidenceEstimated}
			}
		}
	}

	// Activity evidence comes from message.timestamp.type. The actual
	// recency interpretation is M2's job.
	mtype := configString(cfgs, "message.timestamp.type", "CreateTime")
	cleanup := configString(cfgs, "cleanup.policy", "delete")
	retentionMs := configInt64(cfgs, "retention.ms", -1)

	// Tiered-storage detection is delegated to internal/managed (SPEC §5.5)
	// so the rule table lives in one place and is independently tested.
	tieredReason := managed.IsTieredStorage(managed.TieredCheck{
		Configs:     cfgs,
		ClusterType: clusterType,
	})
	remote := tieredReason != managed.TieredNone

	// Flags ── only the structural ones the collector can emit unaided.
	var flags []types.Flag
	if strings.Contains(cleanup, "compact") {
		flags = append(flags, types.FlagCompacted)
	}
	if remote {
		flags = append(flags, types.FlagRemoteStorage)
	}

	// Per-signal MISSING_SIGNAL bookkeeping (Activity / Tenancy / Consumption
	// per SPEC Appendix E).
	var signalsMissing []types.SubSignal
	// Consumption: per-partition list offsets failed for at least one partition.
	if offs.PartitionAuth[name] {
		signalsMissing = append(signalsMissing, types.SubSignalConsumption)
	}
	// Activity: no timestamp returned for any partition AND broker did return
	// data otherwise. We approximate "broker returned data" as len(parts) > 0.
	// SPEC §5.1 says: timestamp absent → UNKNOWN → MISSING_SIGNAL.
	hasTs := offs.LastProduceTs[name] > 0
	if !hasTs && len(parts) > 0 && !offs.PartitionAuth[name] {
		// LogAppendTime brokers always stamp; CreateTime brokers usually do
		// once records exist. An empty topic legitimately has no ts — we do
		// NOT flag empty topics as MISSING_SIGNAL because that would make
		// every brand-new topic INSPECT-capped.
		if earliestSum != latestSum {
			signalsMissing = append(signalsMissing, types.SubSignalActivity)
		}
	}
	// Tenancy: DescribeGroups or FetchManyOffsets denied.
	if groups.DescribeAuth || groups.FetchAuth {
		signalsMissing = append(signalsMissing, types.SubSignalTenancy)
	}
	if len(signalsMissing) > 0 {
		flags = append(flags, types.FlagMissingSignal)
	}

	cgs := groups.PerTopic[name]
	if cgs == nil {
		cgs = []types.ConsumerGroupInfo{}
	}

	// SR view, only when configured. Snapshot field is *pointer* so absence
	// renders as JSON omitempty.
	var srInfo *types.SchemaRegistryInfo
	if sr.Configured {
		if v, ok := sr.PerTopic[name]; ok {
			srInfo = v
		}
	}

	return types.Topic{
		Name:                 name,
		Partitions:           len(tdetail.Partitions),
		ReplicationFactor:    tdetail.Partitions.NumReplicas(),
		CleanupPolicy:        cleanup,
		RetentionMs:          retentionMs,
		RemoteStorageEnabled: remote,
		MessageTimestampType: mtype,
		LastProduceTs:        tsToTime(offs.LastProduceTs[name]),
		EarliestOffsetSum:    earliestSum,
		LatestOffsetSum:      latestSum,
		Storage:              storage,
		PartitionMetrics:     pms,
		ConsumerGroups:       cgs,
		SchemaRegistry:       srInfo,
		Flags:                flags,
		SignalsMissing:       signalsMissing,
	}
}

// buildGlobalMissing returns the scan-level missing_signals_global list. SPEC
// Appendix C: this is a flat string array describing classes of degradation
// observed across the entire cluster.
func buildGlobalMissing(perms types.PermissionsObserved, sr *srResult, warnings []string) []string {
	out := make([]string, 0, 4)
	if !perms.DescribeConfigs {
		out = append(out, "describe_configs_denied")
	}
	if !perms.DescribeGroups {
		out = append(out, "describe_groups_denied")
	}
	if !perms.DescribeLogDirs {
		out = append(out, "describe_log_dirs_unavailable")
	}
	if sr.Configured && !sr.Reachable && !sr.SkippedStrategy {
		out = append(out, "schema_registry_unreachable")
	}
	if sr.SkippedStrategy {
		out = append(out, "schema_registry_strategy_skipped_record_name")
	}
	out = append(out, warnings...)
	return out
}

// configSnapshotFrom mirrors the scoring config into the snapshot per SPEC
// Appendix C. The activity curve translates from config (Score is float) to
// the snapshot type (Score is int rounded).
func configSnapshotFrom(cfg *config.Config) types.ConfigSnapshot {
	w := types.AtticWeights{
		Activity:    cfg.AtticScore.Weights.Activity,
		Tenancy:     cfg.AtticScore.Weights.Tenancy,
		Tonnage:     cfg.AtticScore.Weights.Tonnage,
		Intent:      cfg.AtticScore.Weights.Intent,
		Consumption: cfg.AtticScore.Weights.Consumption,
	}
	th := types.AtticThresholds{
		LikelyUnused: cfg.AtticScore.Thresholds.LikelyUnused,
		Candidate:    cfg.AtticScore.Thresholds.Candidate,
		Inspect:      cfg.AtticScore.Thresholds.Inspect,
	}
	curve := make([]types.ActivityCurvePoint, 0, len(cfg.AtticScore.ActivityCurve))
	for _, p := range cfg.AtticScore.ActivityCurve {
		curve = append(curve, types.ActivityCurvePoint{Days: p.Days, Score: int(p.Score)})
	}
	return types.ConfigSnapshot{AtticWeights: w, Thresholds: th, ActivityCurve: curve}
}
