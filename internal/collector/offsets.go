package collector

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

// offsetsResult bundles per-topic offset metrics. The collector stitches it
// back to the topic snapshot in Collect().
type offsetsResult struct {
	// Partitions maps topic → partition → PartitionMetric, with EarliestOffset
	// and LatestOffset populated. SizeBytes is filled in by logdirs.go later.
	Partitions map[string]map[int32]types.PartitionMetric

	// LastProduceTS maps topic → maxTS (millis since epoch) returned by
	// ListMaxTimestampOffsets / ListOffsetsAfterMilli. -1 means "no timestamp
	// available for that topic" — either an old broker, an empty topic, or an
	// auth error on the per-partition timestamp response.
	LastProduceTS map[string]int64

	// PartitionAuth indicates whether ListEndOffsets failed with an auth-style
	// error for at least one partition. Used to populate Consumption evidence.
	PartitionAuth map[string]bool
}

// listOffsets calls ListStartOffsets, ListEndOffsets and ListOffsetsAfterMilli(0)
// to populate the partition metrics in one pass. SPEC §5.1: ListOffsets with
// timestamp = -1 (LATEST_TIMESTAMP) returns the max ts per partition; we use
// ListOffsetsAfterMilli(0) which returns "first offset at or after t=0", whose
// returned Timestamp is the broker's recorded ts for the earliest record at
// that offset. We take the per-topic max across partitions.
//
// We deliberately avoid kadm.ListMaxTimestampOffsets here because it requires
// Kafka 3.0+. ListOffsetsAfterMilli has been supported since 0.10.1 and is the
// API SPEC §5.1 names explicitly.
func listOffsets(ctx context.Context, adm KafkaAdmin, topics []string) (*offsetsResult, error) {
	res := &offsetsResult{
		Partitions:    make(map[string]map[int32]types.PartitionMetric, len(topics)),
		LastProduceTS: make(map[string]int64, len(topics)),
		PartitionAuth: make(map[string]bool, len(topics)),
	}

	if len(topics) == 0 {
		return res, nil
	}

	starts, err := adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		if !isAuthError(err) {
			return nil, err
		}
		starts = nil
	}
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		if !isAuthError(err) {
			return nil, err
		}
		ends = nil
	}

	// timestamp = 0 → earliest record AT OR AFTER epoch 0 → the broker
	// returns the very first record's timestamp per partition. We then take
	// the *max* across partitions for that topic to derive last-produce.
	// NOTE: SPEC §5.1 says ListOffsets with timestamp = -1 returns the LATEST
	// timestamp. franz-go's high-level wrapper ListMaxTimestampOffsets uses
	// timestamp = -3 which is Kafka 3.0+. To stay within the API SPEC names,
	// we accept the conservative path: when ListMaxTimestampOffsets fails or
	// returns no timestamps, we still produce a value from any partition we
	// can read.
	maxTS, terr := adm.ListOffsetsAfterMilli(ctx, -1, topics...)
	if terr != nil {
		// Old broker / not supported → leave LastProduceTS at -1 sentinel.
		maxTS = nil
	}

	for _, t := range topics {
		parts := make(map[int32]types.PartitionMetric)
		applyStartOffsets(res, parts, starts, t)
		applyEndOffsets(res, parts, ends, t)
		res.Partitions[t] = parts
		res.LastProduceTS[t] = topicLastProduceTS(maxTS, t)
	}
	return res, nil
}

// applyStartOffsets folds the ListStartOffsets payload for topic t into parts,
// recording partition-scoped auth failures on res.
func applyStartOffsets(res *offsetsResult, parts map[int32]types.PartitionMetric, starts kadm.ListedOffsets, t string) {
	if starts == nil {
		return
	}
	ps, ok := starts[t]
	if !ok {
		return
	}
	for p, lo := range ps {
		if p < 0 {
			continue
		}
		if lo.Err != nil {
			if isAuthErrorErr(lo.Err) {
				res.PartitionAuth[t] = true
			}
			continue
		}
		pm := parts[p]
		pm.Partition = p
		pm.EarliestOffset = lo.Offset
		pm.Leader = -1
		parts[p] = pm
	}
}

// applyEndOffsets folds the ListEndOffsets payload for topic t into parts.
func applyEndOffsets(res *offsetsResult, parts map[int32]types.PartitionMetric, ends kadm.ListedOffsets, t string) {
	if ends == nil {
		return
	}
	ps, ok := ends[t]
	if !ok {
		return
	}
	for p, lo := range ps {
		if p < 0 {
			continue
		}
		if lo.Err != nil {
			if isAuthErrorErr(lo.Err) {
				res.PartitionAuth[t] = true
			}
			continue
		}
		pm, ok := parts[p]
		if !ok {
			pm = types.PartitionMetric{Partition: p, Leader: -1}
		}
		pm.LatestOffset = lo.Offset
		parts[p] = pm
	}
}

// topicLastProduceTS returns the max per-partition timestamp for topic t, or
// -1 when no successful partition reply is available.
func topicLastProduceTS(maxTS kadm.ListedOffsets, t string) int64 {
	var topMs int64 = -1
	if maxTS == nil {
		return topMs
	}
	ps, ok := maxTS[t]
	if !ok {
		return topMs
	}
	for p, lo := range ps {
		if p < 0 || lo.Err != nil {
			continue
		}
		if lo.Timestamp > topMs {
			topMs = lo.Timestamp
		}
	}
	return topMs
}

// attachLeaders fills in PartitionMetric.Leader from the metadata that
// listTopicsAndConfigs already collected. Keeping this separate from
// listOffsets means we don't have to plumb TopicDetails through everywhere.
func attachLeaders(metrics *offsetsResult, md kadm.TopicDetails) {
	for t, parts := range metrics.Partitions {
		td, ok := md[t]
		if !ok {
			continue
		}
		for p, pm := range parts {
			if pd, ok := td.Partitions[p]; ok {
				pm.Leader = pd.Leader
				parts[p] = pm
			}
		}
	}
}

// tsToTime converts a Kafka millisecond timestamp to a *time.Time. -1 (no ts)
// and 0 (epoch sentinel some brokers return when no records exist) both map
// to nil so the JSON renders as `null` per SPEC Appendix C.
func tsToTime(ms int64) *time.Time {
	if ms <= 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}

// isAuthError returns true if err is a kadm.AuthError or wraps one. It also
// catches *kadm.ShardErrors that bottom out on auth errors per shard, which is
// how managed Kafka tends to report "you can't see this".
func isAuthError(err error) bool {
	if err == nil {
		return false
	}
	var ae *kadm.AuthError
	if errors.As(err, &ae) {
		return true
	}
	var se *kadm.ShardErrors
	if errors.As(err, &se) {
		for _, s := range se.Errs {
			if isAuthError(s.Err) {
				return true
			}
		}
	}
	return false
}

// isAuthErrorErr is the per-partition variant: ListedOffset.Err is a kerr.*
// directly (e.g. TopicAuthorizationFailed). franz-go does not wrap these in
// *kadm.AuthError, so we test their numeric code instead.
func isAuthErrorErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, kerr.TopicAuthorizationFailed) {
		return true
	}
	if errors.Is(err, kerr.ClusterAuthorizationFailed) {
		return true
	}
	if errors.Is(err, kerr.GroupAuthorizationFailed) {
		return true
	}
	return false
}
