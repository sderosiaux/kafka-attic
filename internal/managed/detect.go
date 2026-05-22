// Package managed implements SPEC §5.5 — Managed Kafka detection and tiered
// storage discovery. It is intentionally side-effect free: every function
// takes pre-collected inputs (bootstrap string, topic configs, log-dir auth
// status) and returns a verdict. No Kafka API calls happen here; the
// collector pipes data in.
//
// The package answers three questions:
//
//  1. detect.go   — what kind of cluster is this? (self-managed, MSK, CC, …)
//  2. tiered.go   — is tiered/remote storage in effect on this topic?
//  3. tonnage_estimate.go — when DescribeLogDirs is restricted, can we still
//     estimate a topic's storage from broker segment metadata alone, never
//     by reading records?
//
// SPEC §5.6 (privacy) forbids record sampling. The estimator here treats
// "average record size" as an input from broker-reported segment metadata
// only; if the caller has nothing better, the result is UNKNOWN.
package managed

import "strings"

// ClusterType is the detected flavor of the connected Kafka cluster. The
// enum is intentionally broad: detection in v1 is heuristic, based on the
// bootstrap address suffix plus permission signals returned by the broker
// itself (e.g. an unauthorized DescribeLogDirs is the strongest hint MSK
// Serverless or Confluent Cloud is in play).
type ClusterType string

const (
	// ClusterUnknown is the default when no heuristic fires. The collector
	// must not assume restricted permissions in this case.
	ClusterUnknown ClusterType = "unknown"

	// ClusterSelfManaged is on-prem / EC2 / k8s-hosted Kafka — full
	// DescribeLogDirs typically available.
	ClusterSelfManaged ClusterType = "self_managed"

	// ClusterMSKProvisioned is AWS MSK provisioned mode. Log-dir works with
	// the right IAM policy.
	ClusterMSKProvisioned ClusterType = "msk_provisioned"

	// ClusterMSKServerless is AWS MSK Serverless. DescribeLogDirs is
	// restricted; Tonnage degrades to UNKNOWN.
	ClusterMSKServerless ClusterType = "msk_serverless"

	// ClusterConfluentCloud is Confluent Cloud. DescribeLogDirs restricted;
	// tiered storage often in effect (`confluent.placement.constraints`,
	// `confluent.tier.enable`, infinite retention).
	ClusterConfluentCloud ClusterType = "confluent_cloud"

	// ClusterAiven is Aiven for Apache Kafka. DescribeLogDirs availability
	// varies by plan; the collector defaults to attempting it.
	ClusterAiven ClusterType = "aiven"

	// ClusterRedpanda is Redpanda — Kafka-protocol compatible, full
	// log-dir support.
	ClusterRedpanda ClusterType = "redpanda"
)

// Display returns a human-readable name for the cluster type, used by the
// renderer.
func (c ClusterType) Display() string {
	switch c {
	case ClusterSelfManaged:
		return "Self-managed Kafka"
	case ClusterMSKProvisioned:
		return "Amazon MSK (provisioned)"
	case ClusterMSKServerless:
		return "Amazon MSK Serverless"
	case ClusterConfluentCloud:
		return "Confluent Cloud"
	case ClusterAiven:
		return "Aiven for Apache Kafka"
	case ClusterRedpanda:
		return "Redpanda"
	default:
		return "Unknown"
	}
}

// DetectInput bundles the few signals the detector needs. Keeping it as a
// struct (rather than positional args) means we can add new heuristics later
// without breaking call sites.
type DetectInput struct {
	// Bootstrap is the raw bootstrap.servers string from the cluster config.
	// Multiple comma-separated hosts are tolerated; we inspect each.
	Bootstrap string

	// DescribeLogDirsUnauthorized is true when DescribeLogDirs failed with
	// an auth-style error during collection. SPEC §5.5: that's the strongest
	// hint we are talking to MSK Serverless or Confluent Cloud.
	DescribeLogDirsUnauthorized bool

	// AnyTopicHasTieredConfig is true when at least one topic in the scan
	// carries `confluent.tier.enable=true` or `confluent.placement.constraints`.
	// That promotes a borderline detection to ConfluentCloud.
	AnyTopicHasTieredConfig bool
}

// Detect returns the best-effort ClusterType for the given input. The
// algorithm:
//
//  1. Suffix-match the bootstrap hosts against a small table of known
//     provider domains. The first match wins.
//  2. If suffix matching yields ClusterUnknown and DescribeLogDirs was
//     unauthorized, return ClusterMSKServerless or ClusterConfluentCloud
//     based on any tiered-storage config seen, else fall back to Unknown.
//
// Detection never panics on malformed input; an empty bootstrap returns
// ClusterUnknown.
func Detect(in DetectInput) ClusterType {
	hosts := splitBootstrap(in.Bootstrap)
	for _, h := range hosts {
		if t := classifyHost(h); t != ClusterUnknown {
			// MSK suffix alone cannot distinguish provisioned vs serverless.
			// Use the log-dir-unauthorized hint as a tiebreaker.
			if t == ClusterMSKProvisioned && in.DescribeLogDirsUnauthorized {
				return ClusterMSKServerless
			}
			return t
		}
	}
	// Fall back to permission-based heuristics when the hostname is opaque
	// (private DNS, IP, custom domain in front of a managed cluster, …).
	if in.DescribeLogDirsUnauthorized {
		if in.AnyTopicHasTieredConfig {
			return ClusterConfluentCloud
		}
		return ClusterUnknown
	}
	return ClusterUnknown
}

// classifyHost maps a single bootstrap host to a ClusterType using a small
// curated suffix table. The matching is case-insensitive and ignores the
// trailing port. Unknown hostnames return ClusterUnknown.
func classifyHost(host string) ClusterType {
	h := strings.ToLower(strings.TrimSpace(host))
	// Drop the port portion (host[:port] or [ipv6]:port).
	if i := strings.LastIndex(h, ":"); i > 0 && !strings.HasSuffix(h[:i], "]") {
		h = h[:i]
	}
	h = strings.TrimSuffix(h, ".")
	switch {
	case h == "":
		return ClusterUnknown
	case strings.HasSuffix(h, ".amazonaws.com"):
		// MSK provisioned brokers expose `b-N.<cluster>.<id>.<region>.amazonaws.com`.
		// MSK Serverless uses the same suffix; the distinction comes from the
		// DescribeLogDirs auth hint, not the hostname.
		return ClusterMSKProvisioned
	case strings.HasSuffix(h, ".confluent.cloud"):
		return ClusterConfluentCloud
	case strings.HasSuffix(h, ".aivencloud.com"):
		return ClusterAiven
	case strings.HasSuffix(h, ".redpanda.com"),
		strings.HasSuffix(h, ".prd.cloud.redpanda.com"),
		strings.HasSuffix(h, ".byoc.prd.cloud.redpanda.com"):
		return ClusterRedpanda
	default:
		return ClusterUnknown
	}
}

// splitBootstrap returns the comma-separated host list from a bootstrap
// string. Leading/trailing whitespace and empty entries are dropped.
func splitBootstrap(bootstrap string) []string {
	if strings.TrimSpace(bootstrap) == "" {
		return nil
	}
	raw := strings.Split(bootstrap, ",")
	out := make([]string, 0, len(raw))
	for _, h := range raw {
		s := strings.TrimSpace(h)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
