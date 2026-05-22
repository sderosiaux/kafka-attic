package collector

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/sderosiaux/kafka-attic/internal/types"
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
	if hasAuthFailure(fetched) {
		res.FetchAuth = true
		// Even with FetchAuth, expose the group states so Tenancy can
		// still distinguish "all dead" from "stable, no offsets visible".
	}

	for _, gname := range groupNames {
		dg, ok := described[gname]
		if !ok || dg.Err != nil {
			continue
		}
		commitsByTopic := commitsByTopicFor(fetched, gname, inScopeTopics)
		topicsForGroup := groupTopics(dg, inScopeTopics, commitsByTopic)
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

// hasAuthFailure returns true when every FetchManyOffsets reply failed and at
// least one of those failures is an auth error.
func hasAuthFailure(fetched kadm.FetchOffsetsResponses) bool {
	if !fetched.AllFailed() {
		return false
	}
	for _, r := range fetched {
		if isAuthError(r.Err) {
			return true
		}
	}
	return false
}

// commitsByTopicFor extracts the per-topic committed-offset sum for group
// gname, restricted to the in-scope topic set. Returns nil when the group's
// fetch failed or is missing — callers treat that as "no commits visible".
func commitsByTopicFor(
	fetched kadm.FetchOffsetsResponses,
	gname string,
	inScopeTopics map[string]struct{},
) map[string]int64 {
	fr, ok := fetched[gname]
	if !ok || fr.Err != nil {
		return nil
	}
	out := make(map[string]int64)
	for topic, parts := range fr.Fetched {
		if _, want := inScopeTopics[topic]; !want {
			continue
		}
		var sum int64
		for _, p := range parts {
			if p.Err != nil || p.At < 0 {
				continue
			}
			sum += p.At
		}
		out[topic] = sum
	}
	return out
}

// groupTopics returns the union of dg.JoinTopics ∩ in-scope plus any topic
// that appeared in commitsByTopic — matching the SPEC §4.2 Tenancy rules
// where a Stable group with no commits still names its topic via JoinTopics.
func groupTopics(
	dg kadm.DescribedGroup,
	inScopeTopics map[string]struct{},
	commitsByTopic map[string]int64,
) map[string]struct{} {
	out := make(map[string]struct{})
	for _, t := range dg.JoinTopics() {
		if _, want := inScopeTopics[t]; want {
			out[t] = struct{}{}
		}
	}
	for t := range commitsByTopic {
		out[t] = struct{}{}
	}
	return out
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

// stableGroupStates lists the Kafka group states the collector recognizes.
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
