package managed

import "testing"

// TestIsTieredStorage_MSKRemoteStorage covers the canonical MSK Tiered
// Storage indicator: `remote.storage.enable=true`. SPEC §5.5 row 1.
func TestIsTieredStorage_MSKRemoteStorage(t *testing.T) {
	got := IsTieredStorage(TieredCheck{
		Configs:     map[string]string{"remote.storage.enable": "true"},
		ClusterType: ClusterMSKProvisioned,
	})
	if got != TieredMSKRemoteStorage {
		t.Fatalf("got %q, want %q", got, TieredMSKRemoteStorage)
	}
}

// TestIsTieredStorage_ConfluentTierEnable covers Confluent Platform tiered
// storage via `confluent.tier.enable=true`. SPEC §5.5 row 2.
func TestIsTieredStorage_ConfluentTierEnable(t *testing.T) {
	got := IsTieredStorage(TieredCheck{
		Configs:     map[string]string{"confluent.tier.enable": "TRUE"},
		ClusterType: ClusterConfluentCloud,
	})
	if got != TieredConfluentTierEnable {
		t.Fatalf("got %q, want %q", got, TieredConfluentTierEnable)
	}
}

// TestIsTieredStorage_ConfluentPlacementConstraints covers the placement
// constraints indicator, which is set to a JSON blob on Confluent Cloud.
// SPEC §5.5 row 2.
func TestIsTieredStorage_ConfluentPlacementConstraints(t *testing.T) {
	got := IsTieredStorage(TieredCheck{
		Configs:     map[string]string{"confluent.placement.constraints": `{"version":2,"replicas":[]}`},
		ClusterType: ClusterConfluentCloud,
	})
	if got != TieredConfluentPlacement {
		t.Fatalf("got %q, want %q", got, TieredConfluentPlacement)
	}
}

// TestIsTieredStorage_InfiniteRetentionOnCC checks the conditional rule:
// retention.ms=-1 alone is NOT tiered, but retention.ms=-1 ON Confluent
// Cloud IS (SPEC §5.5 row 3).
func TestIsTieredStorage_InfiniteRetentionOnCC(t *testing.T) {
	got := IsTieredStorage(TieredCheck{
		Configs:     map[string]string{"retention.ms": "-1"},
		ClusterType: ClusterConfluentCloud,
	})
	if got != TieredInfiniteRetention {
		t.Fatalf("got %q, want %q", got, TieredInfiniteRetention)
	}
}

// TestIsTieredStorage_InfiniteRetentionOnSelfManagedNotTiered locks in the
// asymmetric rule from SPEC §5.5: retention.ms=-1 on a self-managed cluster
// is a valid setting (e.g. compacted state stores) and must NOT trigger
// REMOTE_STORAGE.
func TestIsTieredStorage_InfiniteRetentionOnSelfManagedNotTiered(t *testing.T) {
	got := IsTieredStorage(TieredCheck{
		Configs:     map[string]string{"retention.ms": "-1"},
		ClusterType: ClusterSelfManaged,
	})
	if got != TieredNone {
		t.Fatalf("got %q, want TieredNone", got)
	}
}

// TestIsTieredStorage_OrderingMSKWins documents the detection order in
// SPEC §5.5: the MSK indicator wins over a Confluent placement constraint
// when both are set on the same topic (unlikely in practice, but the order
// of the indicator table is the contract).
func TestIsTieredStorage_OrderingMSKWins(t *testing.T) {
	got := IsTieredStorage(TieredCheck{
		Configs: map[string]string{
			"remote.storage.enable":           "true",
			"confluent.placement.constraints": "anything",
		},
		ClusterType: ClusterConfluentCloud,
	})
	if got != TieredMSKRemoteStorage {
		t.Fatalf("got %q, want %q", got, TieredMSKRemoteStorage)
	}
}

// TestIsTieredStorage_NoIndicator returns TieredNone for a typical
// self-managed topic config.
func TestIsTieredStorage_NoIndicator(t *testing.T) {
	got := IsTieredStorage(TieredCheck{
		Configs: map[string]string{
			"cleanup.policy": "delete",
			"retention.ms":   "604800000",
		},
		ClusterType: ClusterSelfManaged,
	})
	if got != TieredNone {
		t.Fatalf("got %q, want TieredNone", got)
	}
}

// TestIsTieredStorage_EmptyConfigs is the degenerate case the collector
// hits when DescribeTopicConfigs was denied. SPEC §5.3: missing permission
// must not produce false positives.
func TestIsTieredStorage_EmptyConfigs(t *testing.T) {
	if got := IsTieredStorage(TieredCheck{}); got != TieredNone {
		t.Fatalf("got %q, want TieredNone", got)
	}
	if got := IsTieredStorage(TieredCheck{Configs: map[string]string{}}); got != TieredNone {
		t.Fatalf("got %q, want TieredNone", got)
	}
}

// TestIsTieredStorage_EmptyPlacementValueIgnored matches the broker
// behaviour where `confluent.placement.constraints` exists as a key but is
// blank. We must not flag REMOTE_STORAGE on a whitespace-only value.
func TestIsTieredStorage_EmptyPlacementValueIgnored(t *testing.T) {
	got := IsTieredStorage(TieredCheck{
		Configs:     map[string]string{"confluent.placement.constraints": "   "},
		ClusterType: ClusterConfluentCloud,
	})
	if got != TieredNone {
		t.Fatalf("got %q, want TieredNone", got)
	}
}
