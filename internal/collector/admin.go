package collector

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/sderosiaux/kafka-attic/internal/cluster"
)

// KafkaAdmin is the narrow read-only slice of *kadm.Client that the collector
// needs. Extracting it as an interface means tests can swap in a fake without
// dialing a broker, and the production code can never accidentally reach for a
// producer method.
//
// Every method here is a read operation. Adding a write method (CreateTopics,
// DeleteTopics, AlterConfigs, …) is the kind of change readonly_test.go is
// designed to spot.
type KafkaAdmin interface {
	Metadata(ctx context.Context, topics ...string) (kadm.Metadata, error)
	ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
	DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
	ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListOffsetsAfterMilli(ctx context.Context, millisecond int64, topics ...string) (kadm.ListedOffsets, error)
	ListGroups(ctx context.Context, filterStates ...string) (kadm.ListedGroups, error)
	DescribeGroups(ctx context.Context, groups ...string) (kadm.DescribedGroups, error)
	FetchManyOffsets(ctx context.Context, groups ...string) kadm.FetchOffsetsResponses
	DescribeAllLogDirs(ctx context.Context, s kadm.TopicsSet) (kadm.DescribedAllLogDirs, error)
}

// adminFromClients returns the production KafkaAdmin: the real *kadm.Client
// from cluster.Clients. The single line of indirection keeps callers from
// having to know about cluster.Clients internals.
func adminFromClients(c *cluster.Clients) KafkaAdmin {
	return c.Kadm
}
