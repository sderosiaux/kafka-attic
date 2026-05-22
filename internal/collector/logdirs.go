package collector

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"
)

// logDirsResult bundles the per-topic storage size in bytes. NULL is
// represented as "topic absent from the map" — callers must treat that as
// SPEC §4.2 Tonnage `UNKNOWN`.
type logDirsResult struct {
	// BytesByTopic maps topic → aggregate size in bytes (sum across partitions
	// and replicas). The replicated-size question is intentional: SPEC §4.2
	// asks for "storage footprint" and §3.1 / the cleanup script section uses
	// that number to compute "reclaimable TB". Reclaimable storage is the on-
	// disk footprint including replicas, so we sum across brokers.
	BytesByTopic map[string]int64

	// PartitionBytes maps topic → partition → bytes for that one replica
	// (the leader's view when available). Used by the partition_metrics block.
	PartitionBytes map[string]map[int32]int64

	// SegmentBytesByTopic and SegmentRecordCountByTopic carry the optional
	// per-topic segment summary used by the SPEC §5.6 Tonnage estimate:
	//
	//	avg_record_size = segment_bytes / segment_record_count
	//
	// IMPORTANT — none of the Kafka admin APIs exposed by kadm/kmsg today
	// (DescribeLogDirs through KIP-405 / KIP-848) return a segment-level
	// record count. The Kafka wire protocol for DescribeLogDirs gives
	// Size, OffsetLag and IsFuture per partition — and that is all. The
	// only avenues for an average-record-size figure are:
	//
	//   1. broker-side JMX metrics (BrokerTopicMetrics.MessagesInPerSec
	//      paired with LogSize), wired through cfg.Metrics in M7+
	//   2. a vendor-specific summary endpoint we do not target in v1
	//
	// We expose the maps here so the *call shape* into managed.EstimateTonnage
	// is real and unit-testable. In production these maps stay empty until
	// the metrics layer lands, which keeps storage.source="unknown" for
	// MSK Serverless / Confluent Cloud topics — SPEC-compliant skip + weight
	// redistribution per Appendix E.
	SegmentBytesByTopic       map[string]int64
	SegmentRecordCountByTopic map[string]int64

	// Auth is true when DescribeLogDirs failed entirely. In that case both
	// BytesByTopic and PartitionBytes are empty and the Tonnage signal must
	// be reported as UNKNOWN (modulo the estimate path above).
	Auth bool
}

// describeLogDirs calls kadm.DescribeAllLogDirs on the in-scope topics. Auth
// errors degrade the result to "no log-dir data" rather than aborting the
// scan — that's the SPEC §5.5 expectation for MSK Serverless and Confluent
// Cloud where DescribeLogDirs is routinely restricted.
func describeLogDirs(
	ctx context.Context,
	adm KafkaAdmin,
	metrics *offsetsResult,
) (*logDirsResult, error) {
	res := &logDirsResult{
		BytesByTopic:              make(map[string]int64),
		PartitionBytes:            make(map[string]map[int32]int64),
		SegmentBytesByTopic:       make(map[string]int64),
		SegmentRecordCountByTopic: make(map[string]int64),
	}

	if len(metrics.Partitions) == 0 {
		return res, nil
	}

	// Build a TopicsSet that mirrors the partitions we already collected.
	// DescribeAllLogDirs requires the partition list explicitly.
	set := make(kadm.TopicsSet)
	for t, parts := range metrics.Partitions {
		ids := make([]int32, 0, len(parts))
		for p := range parts {
			ids = append(ids, p)
		}
		set.Add(t, ids...)
	}

	all, err := adm.DescribeAllLogDirs(ctx, set)
	if err != nil {
		if isAuthError(err) {
			res.Auth = true
			return res, nil
		}
		// On a hard transport failure (not an auth issue) we still degrade
		// rather than abort: storage is best-effort.
		res.Auth = true
		return res, nil
	}

	// DescribedAllLogDirs is {brokerID → DescribedLogDirs}. Each DescribedLogDir
	// has Topics → partition → DescribedLogDirPartition{Size}.
	//
	// We sum each partition's size across brokers (one entry per replica) to
	// get the total on-disk reclaimable bytes per topic.
	//
	// For PartitionBytes we keep the leader replica's size (or the largest one
	// when we can't decide), so the per-partition snapshot reflects "one
	// replica's worth" — matching the example in SPEC Appendix C where size
	// values fit a single replica.
	leaderBytes := make(map[string]map[int32]int64)
	hasAny := false
	for _, dirsByBroker := range all {
		for _, dir := range dirsByBroker {
			if dir.Err != nil {
				// Per-dir auth error — keep going on the other dirs.
				continue
			}
			for topic, parts := range dir.Topics {
				if res.PartitionBytes[topic] == nil {
					res.PartitionBytes[topic] = make(map[int32]int64)
					leaderBytes[topic] = make(map[int32]int64)
				}
				for pid, dp := range parts {
					if dp.Size < 0 {
						continue
					}
					hasAny = true
					res.BytesByTopic[topic] += dp.Size
					// Keep the max per partition as the "leader-equivalent"
					// figure for partition_metrics.
					if dp.Size > leaderBytes[topic][pid] {
						leaderBytes[topic][pid] = dp.Size
						res.PartitionBytes[topic][pid] = dp.Size
					}
				}
			}
		}
	}

	if !hasAny {
		// Either nothing came back, or every dir entry was an auth error.
		res.Auth = true
	}
	return res, nil
}
