package managed

import "testing"

// TestDetect_BootstrapSuffix walks every supported provider domain and
// asserts the detector returns the matching ClusterType. The unauth hint
// is left false so we are exercising the pure suffix path.
func TestDetect_BootstrapSuffix(t *testing.T) {
	cases := []struct {
		name      string
		bootstrap string
		want      ClusterType
	}{
		{"msk_provisioned", "b-1.demo.abc123.kafka.eu-west-1.amazonaws.com:9098", ClusterMSKProvisioned},
		{"confluent_cloud", "pkc-xyz12.eu-west-1.aws.confluent.cloud:9092", ClusterConfluentCloud},
		{"aiven", "kafka-deadbeef.aivencloud.com:25234", ClusterAiven},
		{"redpanda", "seed-1.byoc.prd.cloud.redpanda.com:9092", ClusterRedpanda},
		{"selfmanaged_unknown", "broker-1.internal:9092", ClusterUnknown},
		{"empty", "", ClusterUnknown},
		{"multi_first_match_wins", "broker.internal:9092,pkc-xyz.eu-west-1.aws.confluent.cloud:9092", ClusterConfluentCloud},
		{"whitespace_tolerated", "   pkc-xyz.eu-west-1.aws.confluent.cloud:9092 ", ClusterConfluentCloud},
		{"case_insensitive", "B-1.DEMO.ABC.KAFKA.US-EAST-1.AMAZONAWS.COM:9098", ClusterMSKProvisioned},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Detect(DetectInput{Bootstrap: c.bootstrap})
			if got != c.want {
				t.Fatalf("Detect(%q) = %q, want %q", c.bootstrap, got, c.want)
			}
		})
	}
}

// TestDetect_MSKServerlessFromAuthHint asserts the provisioned/serverless
// tiebreak: an *.amazonaws.com bootstrap that also returned an unauthorized
// DescribeLogDirs is reported as serverless, matching SPEC §5.5 / Appendix E.
func TestDetect_MSKServerlessFromAuthHint(t *testing.T) {
	got := Detect(DetectInput{
		Bootstrap:                   "boot-abc.c2.kafka-serverless.eu-west-1.amazonaws.com:9098",
		DescribeLogDirsUnauthorized: true,
	})
	if got != ClusterMSKServerless {
		t.Fatalf("got %q, want %q", got, ClusterMSKServerless)
	}
}

// TestDetect_ConfluentCloudFromAuthHint covers the case where the
// bootstrap hostname is opaque (proxy / private link) but the broker's
// DescribeLogDirs denial plus tiered-config evidence points at Confluent
// Cloud. SPEC §5.5: tiered config + log-dir denied → CC.
func TestDetect_ConfluentCloudFromAuthHint(t *testing.T) {
	got := Detect(DetectInput{
		Bootstrap:                   "internal-proxy.acme.corp:9092",
		DescribeLogDirsUnauthorized: true,
		AnyTopicHasTieredConfig:     true,
	})
	if got != ClusterConfluentCloud {
		t.Fatalf("got %q, want %q", got, ClusterConfluentCloud)
	}
}

// TestDetect_OpaqueWithoutHintStaysUnknown locks in conservative behaviour:
// without any positive evidence, the detector must return Unknown. The
// collector relies on this to avoid mis-attributing a self-hosted cluster.
func TestDetect_OpaqueWithoutHintStaysUnknown(t *testing.T) {
	got := Detect(DetectInput{Bootstrap: "broker-1.internal:9092"})
	if got != ClusterUnknown {
		t.Fatalf("got %q, want Unknown", got)
	}
}

// TestClusterType_Display exercises every enum case so the renderer never
// sees a raw `unknown` token leaking through.
func TestClusterType_Display(t *testing.T) {
	cases := map[ClusterType]string{
		ClusterSelfManaged:    "Self-managed Kafka",
		ClusterMSKProvisioned: "Amazon MSK (provisioned)",
		ClusterMSKServerless:  "Amazon MSK Serverless",
		ClusterConfluentCloud: "Confluent Cloud",
		ClusterAiven:          "Aiven for Apache Kafka",
		ClusterRedpanda:       "Redpanda",
		ClusterUnknown:        "Unknown",
	}
	for k, v := range cases {
		if got := k.Display(); got != v {
			t.Errorf("Display(%q) = %q, want %q", k, got, v)
		}
	}
}

// TestSplitBootstrap covers the parser edge cases the detector depends on.
func TestSplitBootstrap(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"a:9092", []string{"a:9092"}},
		{"a:9092,b:9092", []string{"a:9092", "b:9092"}},
		{" a:9092 , , b:9092 ", []string{"a:9092", "b:9092"}},
	}
	for _, c := range cases {
		got := splitBootstrap(c.in)
		if len(got) != len(c.want) {
			t.Fatalf("splitBootstrap(%q) = %v, want %v", c.in, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("splitBootstrap(%q)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}
