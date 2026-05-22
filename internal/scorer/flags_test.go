package scorer

import (
	"slices"
	"testing"

	"github.com/sderosiaux/kafka-attic/internal/config"
	"github.com/sderosiaux/kafka-attic/internal/types"
)

func contains(flags []types.Flag, want types.Flag) bool {
	return slices.Contains(flags, want)
}

func TestFlags_AppearsNeverUsed(t *testing.T) {
	tp := &types.Topic{
		PartitionMetrics: parts([2]int64{0, 0}, [2]int64{0, 0}),
		ConsumerGroups:   []types.ConsumerGroupInfo{},
	}
	out := computeFlags(flagInputs{Topic: tp, GroupsHaveCommit: false}, &config.Config{})
	if !contains(out, types.FlagAppearsNeverUsed) {
		t.Errorf("missing APPEARS_NEVER_USED: %v", out)
	}
}

func TestFlags_AppearsNeverUsed_NotEmittedWhenGroupHasCommit(t *testing.T) {
	tp := &types.Topic{
		PartitionMetrics: parts([2]int64{0, 0}),
		ConsumerGroups: []types.ConsumerGroupInfo{
			{State: "Dead", CommittedOffsetSum: 5},
		},
	}
	out := computeFlags(flagInputs{Topic: tp, GroupsHaveCommit: true}, &config.Config{})
	if contains(out, types.FlagAppearsNeverUsed) {
		t.Errorf("APPEARS_NEVER_USED should not be emitted when a group committed: %v", out)
	}
}

func TestFlags_Purged(t *testing.T) {
	tp := &types.Topic{
		PartitionMetrics: parts([2]int64{10, 10}, [2]int64{50, 50}),
	}
	out := computeFlags(flagInputs{Topic: tp}, &config.Config{})
	if !contains(out, types.FlagPurged) {
		t.Errorf("missing PURGED: %v", out)
	}
}

func TestFlags_OrphanSchema_OnlyTopicDerived(t *testing.T) {
	tp := &types.Topic{}
	// Strategy OK + intent=100 + not skipped → flag.
	out := computeFlags(flagInputs{
		Topic:         tp,
		StrategyOK:    true,
		IntentSkipped: false,
		IntentScore:   100,
	}, &config.Config{})
	if !contains(out, types.FlagOrphanSchema) {
		t.Errorf("missing ORPHAN_SCHEMA: %v", out)
	}
}

func TestFlags_OrphanSchema_NotEmittedOnRecordName(t *testing.T) {
	tp := &types.Topic{}
	// Strategy NOT OK (record_name) → no flag even though intent score=100.
	out := computeFlags(flagInputs{
		Topic:         tp,
		StrategyOK:    false,
		IntentSkipped: true,
		IntentScore:   100,
	}, &config.Config{})
	if contains(out, types.FlagOrphanSchema) {
		t.Errorf("ORPHAN_SCHEMA must not fire for skipped record_name: %v", out)
	}
}

func TestFlags_Oversized_RequiresMetrics(t *testing.T) {
	tp := &types.Topic{}
	// No metrics configured → never emit OVERSIZED.
	out := computeFlags(flagInputs{
		Topic:             tp,
		OversizedFlagged:  true,
		MetricsConfigured: false,
	}, &config.Config{})
	if contains(out, types.FlagOversized) {
		t.Errorf("OVERSIZED must not fire without metrics: %v", out)
	}

	// With metrics + flagged → emit.
	out = computeFlags(flagInputs{
		Topic:             tp,
		OversizedFlagged:  true,
		MetricsConfigured: true,
	}, &config.Config{
		Metrics:   &config.MetricsConfig{Source: "prometheus"},
		Oversized: &config.OversizedConfig{RequiresMetrics: true},
	})
	if !contains(out, types.FlagOversized) {
		t.Errorf("expected OVERSIZED to fire with metrics: %v", out)
	}
}

func TestFlags_Skewed_RequiresSizes(t *testing.T) {
	// Partitions without sizes → no SKEWED.
	tp := &types.Topic{PartitionMetrics: parts([2]int64{0, 100})}
	out := computeFlags(flagInputs{Topic: tp, SkewedFlagged: true}, &config.Config{})
	if contains(out, types.FlagSkewed) {
		t.Errorf("SKEWED must not fire without partition sizes: %v", out)
	}

	// Partitions WITH sizes → SKEWED emitted.
	sz1 := int64(100)
	sz2 := int64(800)
	tp2 := &types.Topic{
		PartitionMetrics: []types.PartitionMetric{
			{Partition: 0, EarliestOffset: 0, LatestOffset: 10, SizeBytes: &sz1},
			{Partition: 1, EarliestOffset: 0, LatestOffset: 100, SizeBytes: &sz2},
		},
	}
	out = computeFlags(flagInputs{Topic: tp2, SkewedFlagged: true}, &config.Config{})
	if !contains(out, types.FlagSkewed) {
		t.Errorf("expected SKEWED with sizes: %v", out)
	}
}

func TestFlags_CompactedPassthrough(t *testing.T) {
	tp := &types.Topic{
		Flags: []types.Flag{types.FlagCompacted},
	}
	out := computeFlags(flagInputs{Topic: tp}, &config.Config{})
	if !contains(out, types.FlagCompacted) {
		t.Errorf("COMPACTED must pass through: %v", out)
	}
}

func TestFlags_RemoteStoragePassthrough(t *testing.T) {
	tp := &types.Topic{
		Flags: []types.Flag{types.FlagRemoteStorage},
	}
	out := computeFlags(flagInputs{Topic: tp}, &config.Config{})
	if !contains(out, types.FlagRemoteStorage) {
		t.Errorf("REMOTE_STORAGE must pass through: %v", out)
	}
}

func TestFlags_MissingSignalFromSignalsMissing(t *testing.T) {
	tp := &types.Topic{
		SignalsMissing: []types.SubSignal{types.SubSignalActivity},
	}
	out := computeFlags(flagInputs{Topic: tp}, &config.Config{})
	if !contains(out, types.FlagMissingSignal) {
		t.Errorf("MISSING_SIGNAL must be emitted: %v", out)
	}
}
