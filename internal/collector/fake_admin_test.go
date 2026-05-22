package collector

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kadm"
)

// fakeAdmin is the canned KafkaAdmin used by every collector unit test. Each
// field is a closure that returns the canned response; absent closures cause
// the corresponding API to return an error (so a test that forgets to wire
// one fails loudly rather than silently returning empty data).
type fakeAdmin struct {
	metadataFn              func(ctx context.Context, topics ...string) (kadm.Metadata, error)
	listTopicsFn            func(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
	describeTopicConfigsFn  func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
	listStartOffsetsFn      func(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	listEndOffsetsFn        func(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	listOffsetsAfterMilliFn func(ctx context.Context, ms int64, topics ...string) (kadm.ListedOffsets, error)
	listGroupsFn            func(ctx context.Context, filterStates ...string) (kadm.ListedGroups, error)
	describeGroupsFn        func(ctx context.Context, groups ...string) (kadm.DescribedGroups, error)
	fetchManyOffsetsFn      func(ctx context.Context, groups ...string) kadm.FetchOffsetsResponses
	describeAllLogDirsFn    func(ctx context.Context, s kadm.TopicsSet) (kadm.DescribedAllLogDirs, error)
}

func (f *fakeAdmin) Metadata(ctx context.Context, topics ...string) (kadm.Metadata, error) {
	if f.metadataFn == nil {
		return kadm.Metadata{}, errors.New("fakeAdmin.Metadata not wired")
	}
	return f.metadataFn(ctx, topics...)
}

func (f *fakeAdmin) ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
	if f.listTopicsFn == nil {
		return nil, errors.New("fakeAdmin.ListTopics not wired")
	}
	return f.listTopicsFn(ctx, topics...)
}

func (f *fakeAdmin) DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
	if f.describeTopicConfigsFn == nil {
		return nil, errors.New("fakeAdmin.DescribeTopicConfigs not wired")
	}
	return f.describeTopicConfigsFn(ctx, topics...)
}

func (f *fakeAdmin) ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	if f.listStartOffsetsFn == nil {
		return nil, errors.New("fakeAdmin.ListStartOffsets not wired")
	}
	return f.listStartOffsetsFn(ctx, topics...)
}

func (f *fakeAdmin) ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	if f.listEndOffsetsFn == nil {
		return nil, errors.New("fakeAdmin.ListEndOffsets not wired")
	}
	return f.listEndOffsetsFn(ctx, topics...)
}

func (f *fakeAdmin) ListOffsetsAfterMilli(ctx context.Context, ms int64, topics ...string) (kadm.ListedOffsets, error) {
	if f.listOffsetsAfterMilliFn == nil {
		return nil, errors.New("fakeAdmin.ListOffsetsAfterMilli not wired")
	}
	return f.listOffsetsAfterMilliFn(ctx, ms, topics...)
}

func (f *fakeAdmin) ListGroups(ctx context.Context, filterStates ...string) (kadm.ListedGroups, error) {
	if f.listGroupsFn == nil {
		return nil, errors.New("fakeAdmin.ListGroups not wired")
	}
	return f.listGroupsFn(ctx, filterStates...)
}

func (f *fakeAdmin) DescribeGroups(ctx context.Context, groups ...string) (kadm.DescribedGroups, error) {
	if f.describeGroupsFn == nil {
		return nil, errors.New("fakeAdmin.DescribeGroups not wired")
	}
	return f.describeGroupsFn(ctx, groups...)
}

func (f *fakeAdmin) FetchManyOffsets(ctx context.Context, groups ...string) kadm.FetchOffsetsResponses {
	if f.fetchManyOffsetsFn == nil {
		return kadm.FetchOffsetsResponses{}
	}
	return f.fetchManyOffsetsFn(ctx, groups...)
}

func (f *fakeAdmin) DescribeAllLogDirs(ctx context.Context, s kadm.TopicsSet) (kadm.DescribedAllLogDirs, error) {
	if f.describeAllLogDirsFn == nil {
		return nil, errors.New("fakeAdmin.DescribeAllLogDirs not wired")
	}
	return f.describeAllLogDirsFn(ctx, s)
}

// authError builds a *kadm.AuthError wrapping the given underlying error.
// Useful when the test wants to assert the collector's degraded behaviour.
func authError(inner error) error {
	return &kadm.AuthError{Err: inner}
}
