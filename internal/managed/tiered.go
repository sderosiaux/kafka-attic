package managed

import "strings"

// TieredReason describes why a topic was flagged REMOTE_STORAGE. The
// renderer surfaces it verbatim in the per-topic "notes" column when the
// flag is present, so the user knows which config key triggered.
type TieredReason string

const (
	// TieredNone — no tiered-storage indicator detected.
	TieredNone TieredReason = ""

	// TieredMSKRemoteStorage — `remote.storage.enable=true`. SPEC §5.5.
	TieredMSKRemoteStorage TieredReason = "remote.storage.enable=true"

	// TieredConfluentTierEnable — `confluent.tier.enable=true`. SPEC §5.5.
	TieredConfluentTierEnable TieredReason = "confluent.tier.enable=true"

	// TieredConfluentPlacement — `confluent.placement.constraints` set.
	// SPEC §5.5.
	TieredConfluentPlacement TieredReason = "confluent.placement.constraints"

	// TieredInfiniteRetention — `retention.ms=-1` AND broker detected as
	// Confluent Cloud. SPEC §5.5.
	TieredInfiniteRetention TieredReason = "retention.ms=-1 on Confluent Cloud"
)

// TieredCheck is the per-topic input the detector needs.
type TieredCheck struct {
	// Configs is the topic-config map (key→value, broker-reported strings).
	// nil is treated as "no configs available" and yields TieredNone.
	Configs map[string]string

	// ClusterType is the detected flavor for the connected cluster. It is
	// required to decide whether `retention.ms=-1` should count as infinite
	// retention — only Confluent Cloud uses that idiom intentionally.
	ClusterType ClusterType
}

// IsTieredStorage returns the first matching TieredReason for the topic, or
// TieredNone if no indicator was found. Detection order mirrors SPEC §5.5:
//
//  1. remote.storage.enable=true        → MSK tiered storage
//  2. confluent.tier.enable=true        → Confluent Cloud / Platform tiered
//  3. confluent.placement.constraints   → Confluent Cloud placement
//  4. retention.ms=-1 on Confluent Cloud → infinite retention
//
// The function is deterministic and does not allocate when the topic is not
// tiered.
func IsTieredStorage(in TieredCheck) TieredReason {
	if len(in.Configs) == 0 {
		return TieredNone
	}
	if boolConfig(in.Configs, "remote.storage.enable") {
		return TieredMSKRemoteStorage
	}
	if boolConfig(in.Configs, "confluent.tier.enable") {
		return TieredConfluentTierEnable
	}
	if v, ok := in.Configs["confluent.placement.constraints"]; ok && strings.TrimSpace(v) != "" {
		return TieredConfluentPlacement
	}
	// retention.ms = -1 is "keep forever". On self-managed Kafka that is a
	// legitimate setting (used by compacted state stores, audit logs, etc).
	// SPEC §5.5 narrows the heuristic to Confluent Cloud where -1 is the
	// shorthand for "tiered, infinite retention".
	if in.ClusterType == ClusterConfluentCloud {
		if v, ok := in.Configs["retention.ms"]; ok && strings.TrimSpace(v) == "-1" {
			return TieredInfiniteRetention
		}
	}
	return TieredNone
}

// boolConfig parses a broker-string boolean. Same logic as the collector's
// internal helper but kept private so this package has no dependency on
// internal/collector.
func boolConfig(m map[string]string, key string) bool {
	v, ok := m[key]
	if !ok {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(v), "true")
}
