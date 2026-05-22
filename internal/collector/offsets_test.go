package collector

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// TestListOffsets_HappyPath wires three partitions with monotonic start/end
// offsets and per-partition timestamps, and asserts the per-topic max ts is
// chosen correctly.
func TestListOffsets_HappyPath(t *testing.T) {
	adm := &fakeAdmin{
		listStartOffsetsFn: func(_ context.Context, _ ...string) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{
				"orders": {
					0: {Topic: "orders", Partition: 0, Offset: 100},
					1: {Topic: "orders", Partition: 1, Offset: 200},
					2: {Topic: "orders", Partition: 2, Offset: 300},
				},
			}, nil
		},
		listEndOffsetsFn: func(_ context.Context, _ ...string) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{
				"orders": {
					0: {Topic: "orders", Partition: 0, Offset: 1500},
					1: {Topic: "orders", Partition: 1, Offset: 2500},
					2: {Topic: "orders", Partition: 2, Offset: 3500},
				},
			}, nil
		},
		listOffsetsAfterMilliFn: func(_ context.Context, _ int64, _ ...string) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{
				"orders": {
					0: {Topic: "orders", Partition: 0, Offset: 1499, Timestamp: 1_700_000_000_000},
					1: {Topic: "orders", Partition: 1, Offset: 2499, Timestamp: 1_750_000_000_000}, // newest
					2: {Topic: "orders", Partition: 2, Offset: 3499, Timestamp: 1_720_000_000_000},
				},
			}, nil
		},
	}

	res, err := listOffsets(context.Background(), adm, []string{"orders"})
	if err != nil {
		t.Fatalf("listOffsets returned err: %v", err)
	}
	parts := res.Partitions["orders"]
	if len(parts) != 3 {
		t.Fatalf("want 3 partitions, got %d", len(parts))
	}
	if parts[0].EarliestOffset != 100 || parts[0].LatestOffset != 1500 {
		t.Fatalf("partition 0 wrong offsets: %+v", parts[0])
	}
	if res.LastProduceTs["orders"] != 1_750_000_000_000 {
		t.Fatalf("expected max ts 1_750_000_000_000, got %d", res.LastProduceTs["orders"])
	}
	if res.PartitionAuth["orders"] {
		t.Fatal("partition auth flag should be false on happy path")
	}
}

// TestListOffsets_PartialAuthError marks one partition as TopicAuthorizationFailed.
// The collector must keep the other partitions and flag the topic as
// PartitionAuth so the orchestrator can record MISSING_SIGNAL for Consumption.
func TestListOffsets_PartialAuthError(t *testing.T) {
	adm := &fakeAdmin{
		listStartOffsetsFn: func(_ context.Context, _ ...string) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{
				"orders": {
					0: {Topic: "orders", Partition: 0, Offset: 0, Err: kerr.TopicAuthorizationFailed},
					1: {Topic: "orders", Partition: 1, Offset: 200},
				},
			}, nil
		},
		listEndOffsetsFn: func(_ context.Context, _ ...string) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{
				"orders": {
					1: {Topic: "orders", Partition: 1, Offset: 2500},
				},
			}, nil
		},
		listOffsetsAfterMilliFn: func(_ context.Context, _ int64, _ ...string) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{
				"orders": {
					1: {Topic: "orders", Partition: 1, Offset: 2499, Timestamp: 1_700_000_000_000},
				},
			}, nil
		},
	}

	res, err := listOffsets(context.Background(), adm, []string{"orders"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !res.PartitionAuth["orders"] {
		t.Fatal("expected PartitionAuth=true")
	}
	if _, ok := res.Partitions["orders"][1]; !ok {
		t.Fatal("partition 1 should still be present")
	}
}

// TestListOffsets_NoTimestamp emulates an old broker where the
// ListOffsetsAfterMilli call errors out. LastProduceTs should fall back to -1.
func TestListOffsets_NoTimestamp(t *testing.T) {
	adm := &fakeAdmin{
		listStartOffsetsFn: func(_ context.Context, _ ...string) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{"old-topic": {0: {Topic: "old-topic", Partition: 0, Offset: 0}}}, nil
		},
		listEndOffsetsFn: func(_ context.Context, _ ...string) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{"old-topic": {0: {Topic: "old-topic", Partition: 0, Offset: 0}}}, nil
		},
		listOffsetsAfterMilliFn: func(_ context.Context, _ int64, _ ...string) (kadm.ListedOffsets, error) {
			return nil, errors.New("broker too old")
		},
	}

	res, err := listOffsets(context.Background(), adm, []string{"old-topic"})
	if err != nil {
		t.Fatalf("listOffsets returned err: %v", err)
	}
	if res.LastProduceTs["old-topic"] != -1 {
		t.Fatalf("expected -1 sentinel, got %d", res.LastProduceTs["old-topic"])
	}
}

// TestListOffsets_AuthOnStart covers the full-auth-error path: ListStartOffsets
// fails with an AuthError. The collector swallows it and keeps the topic in
// the result with empty partition metrics.
func TestListOffsets_AuthOnStart(t *testing.T) {
	adm := &fakeAdmin{
		listStartOffsetsFn: func(_ context.Context, _ ...string) (kadm.ListedOffsets, error) {
			return nil, authError(errors.New("denied"))
		},
		listEndOffsetsFn: func(_ context.Context, _ ...string) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{}, nil
		},
		listOffsetsAfterMilliFn: func(_ context.Context, _ int64, _ ...string) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{}, nil
		},
	}

	res, err := listOffsets(context.Background(), adm, []string{"protected"})
	if err != nil {
		t.Fatalf("unexpected hard err: %v", err)
	}
	if len(res.Partitions["protected"]) != 0 {
		t.Fatalf("expected empty partitions, got %v", res.Partitions["protected"])
	}
}

// TestTsToTime confirms the boundary cases of the ts→time helper:
// -1 → nil, 0 → nil, positive → non-nil UTC.
func TestTsToTime(t *testing.T) {
	if tsToTime(-1) != nil {
		t.Fatal("ts=-1 must be nil")
	}
	if tsToTime(0) != nil {
		t.Fatal("ts=0 must be nil")
	}
	v := tsToTime(1_700_000_000_000)
	if v == nil {
		t.Fatal("positive ts must be non-nil")
	}
	if v.Location().String() != "UTC" {
		t.Fatalf("expected UTC, got %s", v.Location().String())
	}
}
