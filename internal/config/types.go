package config

// Config mirrors the kattic.yaml structure described in SPEC Appendix B.
// Only fields required for M0 (auth + scoring weights/thresholds) are wired
// into behaviour; the rest is parsed and round-tripped so later milestones
// can consume it without breaking the file format.
type Config struct {
	Cluster                ClusterConfig          `yaml:"cluster" mapstructure:"cluster"`
	SchemaRegistry         *SchemaRegistryConfig  `yaml:"schema_registry,omitempty" mapstructure:"schema_registry"`
	Owners                 *OwnersConfig          `yaml:"owners,omitempty" mapstructure:"owners"`
	AtticScore             AtticScoreConfig       `yaml:"attic_score" mapstructure:"attic_score"`
	Oversized              *OversizedConfig       `yaml:"oversized,omitempty" mapstructure:"oversized"`
	Skew                   *SkewConfig            `yaml:"skew,omitempty" mapstructure:"skew"`
	Metrics                *MetricsConfig         `yaml:"metrics,omitempty" mapstructure:"metrics"`
	History                *HistoryConfig         `yaml:"history,omitempty" mapstructure:"history"`
	ExcludePatterns        *ExcludePatternsConfig `yaml:"exclude_patterns,omitempty" mapstructure:"exclude_patterns"`
	ProtectedCleanupPolicy []string               `yaml:"protected_cleanup_policies,omitempty" mapstructure:"protected_cleanup_policies"`
	Telemetry              *TelemetryConfig       `yaml:"telemetry,omitempty" mapstructure:"telemetry"`
	Report                 *ReportConfig          `yaml:"report,omitempty" mapstructure:"report"`
}

// ClusterConfig identifies a single Kafka cluster.
type ClusterConfig struct {
	Name      string     `yaml:"name" mapstructure:"name"`
	Bootstrap string     `yaml:"bootstrap" mapstructure:"bootstrap"`
	Auth      AuthConfig `yaml:"auth" mapstructure:"auth"`
}

// AuthType enumerates the supported authentication mechanisms.
type AuthType string

// AuthType enum values.
const (
	AuthNone      AuthType = "none"
	AuthSASLPlain AuthType = "sasl_plain"
	AuthSCRAM     AuthType = "scram"
	AuthMTLS      AuthType = "mtls"
	AuthIAM       AuthType = "iam"
	AuthOAuth     AuthType = "oauth"
)

// AuthConfig is the discriminated-union of auth shapes. Only one block is
// populated per cluster, determined by Type.
type AuthConfig struct {
	Type AuthType `yaml:"type" mapstructure:"type"`

	// TLS toggles are shared by every auth type. mtls always implies TLS.
	TLS *TLSConfig `yaml:"tls,omitempty" mapstructure:"tls"`

	// sasl_plain
	UsernameEnv string `yaml:"username_env,omitempty" mapstructure:"username_env"`
	PasswordEnv string `yaml:"password_env,omitempty" mapstructure:"password_env"`

	// scram
	Mechanism string `yaml:"mechanism,omitempty" mapstructure:"mechanism"` // SCRAM-SHA-256 | SCRAM-SHA-512

	// iam
	Region        string             `yaml:"region,omitempty" mapstructure:"region"`
	Profile       *string            `yaml:"profile,omitempty" mapstructure:"profile"`
	AssumeRoleARN *string            `yaml:"assume_role_arn,omitempty" mapstructure:"assume_role_arn"`
	WebIdentity   *WebIdentityConfig `yaml:"web_identity,omitempty" mapstructure:"web_identity"`

	// oauth
	TokenEndpoint   string `yaml:"token_endpoint,omitempty" mapstructure:"token_endpoint"`
	ClientIDEnv     string `yaml:"client_id_env,omitempty" mapstructure:"client_id_env"`
	ClientSecretEnv string `yaml:"client_secret_env,omitempty" mapstructure:"client_secret_env"`
	Scope           string `yaml:"scope,omitempty" mapstructure:"scope"`
}

// TLSConfig configures broker-side and client-side TLS material.
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled" mapstructure:"enabled"`
	CAFile             string `yaml:"ca_file,omitempty" mapstructure:"ca_file"`
	CertFile           string `yaml:"cert_file,omitempty" mapstructure:"cert_file"`
	KeyFile            string `yaml:"key_file,omitempty" mapstructure:"key_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty" mapstructure:"insecure_skip_verify"`
	ServerName         string `yaml:"server_name,omitempty" mapstructure:"server_name"`
}

// WebIdentityConfig configures EKS IRSA / OIDC token-file based assume role.
type WebIdentityConfig struct {
	RoleARN         string `yaml:"role_arn" mapstructure:"role_arn"`
	TokenFile       string `yaml:"token_file" mapstructure:"token_file"`
	RoleSessionName string `yaml:"role_session_name,omitempty" mapstructure:"role_session_name"`
}

// SchemaRegistryConfig — optional Confluent SR integration.
type SchemaRegistryConfig struct {
	Provider        string       `yaml:"provider" mapstructure:"provider"`
	URL             string       `yaml:"url" mapstructure:"url"`
	Auth            SRAuthConfig `yaml:"auth" mapstructure:"auth"`
	SubjectStrategy string       `yaml:"subject_strategy" mapstructure:"subject_strategy"`
	OnFailure       string       `yaml:"on_failure" mapstructure:"on_failure"`
}

// SRAuthConfig — basic or bearer auth on top of the SR HTTP endpoint.
type SRAuthConfig struct {
	Type        string `yaml:"type" mapstructure:"type"`
	UsernameEnv string `yaml:"username_env,omitempty" mapstructure:"username_env"`
	PasswordEnv string `yaml:"password_env,omitempty" mapstructure:"password_env"`
	TokenEnv    string `yaml:"token_env,omitempty" mapstructure:"token_env"`
}

// OwnersConfig — sources and precedence for owner resolution.
type OwnersConfig struct {
	Precedence  []string               `yaml:"precedence" mapstructure:"precedence"`
	File        *OwnersFileConfig      `yaml:"file,omitempty" mapstructure:"file"`
	TopicConfig *OwnersTopicConfig     `yaml:"topic_config,omitempty" mapstructure:"topic_config"`
	Backstage   *OwnersBackstageConfig `yaml:"backstage,omitempty" mapstructure:"backstage"`
	JSON        *OwnersJSONConfig      `yaml:"json,omitempty" mapstructure:"json"`
}

// OwnersFileConfig configures a local YAML/JSON file owner source.
type OwnersFileConfig struct {
	Path string `yaml:"path" mapstructure:"path"`
}

// OwnersTopicConfig configures owner extraction from a Kafka topic config key.
type OwnersTopicConfig struct {
	Key string `yaml:"key" mapstructure:"key"`
}

// OwnersBackstageConfig configures the Backstage catalog owner source.
type OwnersBackstageConfig struct {
	URL              string       `yaml:"url" mapstructure:"url"`
	Auth             SRAuthConfig `yaml:"auth" mapstructure:"auth"`
	EntityPattern    string       `yaml:"entity_pattern" mapstructure:"entity_pattern"`
	FallbackRelation string       `yaml:"fallback_relation" mapstructure:"fallback_relation"`
}

// OwnersJSONConfig configures a generic JSON HTTP owner source.
type OwnersJSONConfig struct {
	URL     string            `yaml:"url" mapstructure:"url"`
	Headers map[string]string `yaml:"headers" mapstructure:"headers"`
	Extract string            `yaml:"extract" mapstructure:"extract"`
}

// AtticScoreConfig — weights, thresholds and the piecewise activity curve.
type AtticScoreConfig struct {
	SpecVersion   string               `yaml:"spec_version" mapstructure:"spec_version"`
	Weights       Weights              `yaml:"weights" mapstructure:"weights"`
	Thresholds    Thresholds           `yaml:"thresholds" mapstructure:"thresholds"`
	ActivityCurve []ActivityCurvePoint `yaml:"activity_curve" mapstructure:"activity_curve"`
}

// Weights — must sum to 1.0 within tolerance.
type Weights struct {
	Activity    float64 `yaml:"activity" mapstructure:"activity"`
	Tenancy     float64 `yaml:"tenancy" mapstructure:"tenancy"`
	Tonnage     float64 `yaml:"tonnage" mapstructure:"tonnage"`
	Intent      float64 `yaml:"intent" mapstructure:"intent"`
	Consumption float64 `yaml:"consumption" mapstructure:"consumption"`
}

// Sum returns the total weight; useful for validation and redistribution.
func (w Weights) Sum() float64 {
	return w.Activity + w.Tenancy + w.Tonnage + w.Intent + w.Consumption
}

// Thresholds — must satisfy likely_unused > candidate > inspect.
type Thresholds struct {
	LikelyUnused int `yaml:"likely_unused" mapstructure:"likely_unused"`
	Candidate    int `yaml:"candidate" mapstructure:"candidate"`
	Inspect      int `yaml:"inspect" mapstructure:"inspect"`
}

// ActivityCurvePoint — one point on the piecewise-linear days → sub-score curve.
type ActivityCurvePoint struct {
	Days  int     `yaml:"days" mapstructure:"days"`
	Score float64 `yaml:"score" mapstructure:"score"`
}

// OversizedConfig governs the OVERSIZED flag detector.
type OversizedConfig struct {
	MaxPartitionsForThroughput int  `yaml:"max_partitions_for_throughput" mapstructure:"max_partitions_for_throughput"`
	LowTrafficMsgsPerSec       int  `yaml:"low_traffic_msgs_per_sec" mapstructure:"low_traffic_msgs_per_sec"`
	RequiresMetrics            bool `yaml:"requires_metrics" mapstructure:"requires_metrics"`
}

// SkewConfig governs the SKEWED flag detector.
type SkewConfig struct {
	MaxRatioToAverage float64 `yaml:"max_ratio_to_average" mapstructure:"max_ratio_to_average"`
}

// MetricsConfig selects the optional external metrics source.
type MetricsConfig struct {
	Source     string                   `yaml:"source" mapstructure:"source"`
	Prometheus *MetricsPrometheusConfig `yaml:"prometheus,omitempty" mapstructure:"prometheus"`
}

// MetricsPrometheusConfig configures the Prometheus metrics source.
type MetricsPrometheusConfig struct {
	URL             string `yaml:"url" mapstructure:"url"`
	QueryMsgsPerSec string `yaml:"query_msgs_per_sec" mapstructure:"query_msgs_per_sec"`
}

// HistoryConfig configures the local SQLite history store.
type HistoryConfig struct {
	Enabled       bool   `yaml:"enabled" mapstructure:"enabled"`
	Path          string `yaml:"path" mapstructure:"path"`
	RetentionDays int    `yaml:"retention_days" mapstructure:"retention_days"`
}

// ExcludePatternsConfig configures topic-name exclusion patterns.
type ExcludePatternsConfig struct {
	Defaults   bool     `yaml:"defaults" mapstructure:"defaults"`
	Effect     string   `yaml:"effect" mapstructure:"effect"`
	Additional []string `yaml:"additional" mapstructure:"additional"`
}

// TelemetryConfig configures anonymous telemetry pings.
type TelemetryConfig struct {
	Enabled                 bool   `yaml:"enabled" mapstructure:"enabled"`
	Endpoint                string `yaml:"endpoint" mapstructure:"endpoint"`
	IncludeAnonymousRunUUID bool   `yaml:"include_anonymous_run_uuid" mapstructure:"include_anonymous_run_uuid"`
}

// ReportConfig configures report rendering and output.
type ReportConfig struct {
	Format               string            `yaml:"format" mapstructure:"format"`
	Output               string            `yaml:"output" mapstructure:"output"`
	IncludeCleanupScript bool              `yaml:"include_cleanup_script" mapstructure:"include_cleanup_script"`
	RedactTopicNames     string            `yaml:"redact_topic_names" mapstructure:"redact_topic_names"`
	UTM                  map[string]string `yaml:"utm" mapstructure:"utm"`
}
