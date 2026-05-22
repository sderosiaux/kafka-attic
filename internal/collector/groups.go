package collector

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/conduktor/kafka-attic/internal/types"
)

// groupsResult bundles the per-topic view of consumer groups for the
// Tenancy + lag computation.
//
// PerTopic maps topic name → slice of ConsumerGroupInfo (one entry per group
// that has committed offsets on the topic). The slice is sorted by group_id.
type groupsResult struct {
	PerTopic map[string][]types.ConsumerGroupInfo

	// DescribeAuth and FetchAuth are set true when the corresponding API
	// failed for the cluster. Callers use them to record per-topic
	// MISSING_SIGNAL / Tenancy=UNKNOWN.
	DescribeAuth bool
	FetchAuth    bool
}

// listAndDescribeGroups enumerates groups, describes them, and fetches their
// committed offsets. The lag is computed against latestPerPartition (latest
// offset minus committed offset, summed per topic).
//
// The pass keeps every group state verbatim (Stable / PreparingRebalance /
// CompletingRebalance / Empty / Dead). The scorer (M2) reads these strings.
func listAndDescribeGroups(
	ctx context.Context,
	adm KafkaAdmin,
	inScopeTopics map[string]struct{},
	latest map[string]map[int32]int64,
) (*groupsResult, error) {
	res := &groupsResult{PerTopic: make(map[string][]types.ConsumerGroupInfo)}

	listed, err := adm.ListGroups(ctx)
	if err != nil {
		if isAuthError(err) {
			res.DescribeAuth = true
			return res, nil
		}
		return nil, err
	}
	groupNames := listed.Groups()
	if len(groupNames) == 0 {
		return res, nil
	}

	described, err := adm.DescribeGroups(ctx, groupNames...)
	if err != nil {
		if isAuthError(err) {
			res.DescribeAuth = true
			return res, nil
		}
		return nil, err
	}

	fetched := adm.FetchManyOffsets(ctx, groupNames...)
	// Sniff for auth on the offset side. AllFailed plus all errs being auth =
	// definite no-access; otherwise we treat partial failures as best-effort.
	if fetched.AllFailed() {
		anyAuth := false
		for _, r := range fetched {
			if isAuthError(r.Err) {
				anyAuth = true
				break
			}
		}
		if anyAuth {
			res.FetchAuth = true
			// Even with FetchAuth, expose the group states so Tenancy can
			// still distinguish "all dead" from "stable, no offsets visible".
			// Fall through and continue without offsets.
		}
	}

	for _, gname := range groupNames {
		dg, ok := described[gname]
		if !ok || dg.Err != nil {
			continue
		}
		// Committed offsets (per topic) for this group.
		fr, hasFetch := fetched[gname]
		var commitsByTopic map[string]int64
		if hasFetch && fr.Err == nil {
			commitsByTopic = make(map[string]int64)
			for topic, parts := range fr.Fetched {
				if _, want := inScopeTopics[topic]; !want {
					continue
				}
				var sum int64
				for _, p := range parts {
					if p.Err != nil {
						continue
					}
					if p.At < 0 {
						continue
					}
					sum += p.At
				}
				commitsByTopic[topic] = sum
			}
		}

		// Always include topics named via JoinTopics — that's how Tenancy
		// sees a Stable group with no commits yet (rule #1 in §4.2). For
		// Empty/Dead groups (no members) we fall back to commitsByTopic.
		topicsForGroup := make(map[string]struct{})
		for _, t := range dg.JoinTopics() {
			if _, want := inScopeTopics[t]; want {
				topicsForGroup[t] = struct{}{}
			}
		}
		for t := range commitsByTopic {
			topicsForGroup[t] = struct{}{}
		}

		memberCount := len(dg.Members)
		for topic := range topicsForGroup {
			committed := commitsByTopic[topic]
			lag := computeLag(topic, committed, latest[topic])
			res.PerTopic[topic] = append(res.PerTopic[topic], types.ConsumerGroupInfo{
				GroupID:            gname,
				State:              dg.State,
				MemberCount:        memberCount,
				CommittedOffsetSum: committed,
				LagSum:             lag,
			})
		}
	}

	for t, gs := range res.PerTopic {
		sort.Slice(gs, func(i, j int) bool { return gs[i].GroupID < gs[j].GroupID })
		res.PerTopic[t] = gs
	}
	return res, nil
}

// computeLag returns max(0, sum(latest) - committed). When latest is unknown
// (no partition map) we return 0 — the lag is uncomputable rather than
// negative.
//
// NOTE: when a group commits per-partition, we already summed commits before
// calling this. The lag formula in SPEC §5.2 is sum(latest - committed); we
// do it on the sums because per-partition committed → latest mapping is not
// preserved past the FetchManyOffsets step. This matches SPEC §4.2 Tenancy
// rules which only ask "is committed < latest" at the topic level.
func computeLag(_ string, committedSum int64, latest map[int32]int64) int64 {
	if latest == nil {
		return 0
	}
	var total int64
	for _, v := range latest {
		if v > 0 {
			total += v
		}
	}
	lag := total - committedSum
	if lag < 0 {
		return 0
	}
	return lag
}

// latestPerPartitionFromMetrics extracts {topic → {partition → latestOffset}}
// from the offsets stage so the groups stage can compute lag without dialing
// the broker a second time.
func latestPerPartitionFromMetrics(metrics *offsetsResult) map[string]map[int32]int64 {
	out := make(map[string]map[int32]int64, len(metrics.Partitions))
	for t, parts := range metrics.Partitions {
		inner := make(map[int32]int64, len(parts))
		for p, pm := range parts {
			inner[p] = pm.LatestOffset
		}
		out[t] = inner
	}
	return out
}

// stableGroupStates lists the Kafka group states the collector recognises.
// Kept as a comment + exported constants so callers (M2 scorer) reference the
// same string identifiers and the test suite can drive every variant.
const (
	GroupStateStable              = "Stable"
	GroupStatePreparingRebalance  = "PreparingRebalance"
	GroupStateCompletingRebalance = "CompletingRebalance"
	GroupStateEmpty               = "Empty"
	GroupStateDead                = "Dead"
)

// Guard against an unused-import situation on linkers that don't fold the
// kadm import when none of the symbols above touch it directly.
var _ = kadm.TopicsSet(nil)
