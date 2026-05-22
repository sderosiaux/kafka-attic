package config

// Config mirrors the kattic.yaml structure described in SPEC Appendix B.
// Only fields required for M0 (auth + scoring weights/thresholds) are wired
// into behavior; the rest is parsed and round-tripped so later milestones
// can consume it without breaking the file format.
type Config struct {
	Cluster                ClusterConfig          `mapstructure:"cluster"                    yaml:"cluster"`
	SchemaRegistry         *SchemaRegistryConfig  `mapstructure:"schema_registry"            yaml:"schema_registry,omitempty"`
	Owners                 *OwnersConfig          `mapstructure:"owners"                     yaml:"owners,omitempty"`
	AtticScore             AtticScoreConfig       `mapstructure:"attic_score"                yaml:"attic_score"`
	Oversized              *OversizedConfig       `mapstructure:"oversized"                  yaml:"oversized,omitempty"`
	Skew                   *SkewConfig            `mapstructure:"skew"                       yaml:"skew,omitempty"`
	Metrics                *MetricsConfig         `mapstructure:"metrics"                    yaml:"metrics,omitempty"`
	History                *HistoryConfig         `mapstructure:"history"                    yaml:"history,omitempty"`
	ExcludePatterns        *ExcludePatternsConfig `mapstructure:"exclude_patterns"           yaml:"exclude_patterns,omitempty"`
	ProtectedCleanupPolicy []string               `mapstructure:"protected_cleanup_policies" yaml:"protected_cleanup_policies,omitempty"`
	Telemetry              *TelemetryConfig       `mapstructure:"telemetry"                  yaml:"telemetry,omitempty"`
	Report                 *ReportConfig          `mapstructure:"report"                     yaml:"report,omitempty"`
}

// ClusterConfig identifies a single Kafka cluster.
type ClusterConfig struct {
	Name      string     `mapstructure:"name"      yaml:"name"`
	Bootstrap string     `mapstructure:"bootstrap" yaml:"bootstrap"`
	Auth      AuthConfig `mapstructure:"auth"      yaml:"auth"`
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
	Type AuthType `mapstructure:"type" yaml:"type"`

	// TLS toggles are shared by every auth type. mtls always implies TLS.
	TLS *TLSConfig `mapstructure:"tls" yaml:"tls,omitempty"`

	// sasl_plain
	UsernameEnv string `mapstructure:"username_env" yaml:"username_env,omitempty"`
	PasswordEnv string `mapstructure:"password_env" yaml:"password_env,omitempty"`

	// scram
	Mechanism string `mapstructure:"mechanism" yaml:"mechanism,omitempty"` // SCRAM-SHA-256 | SCRAM-SHA-512

	// iam
	Region        string             `mapstructure:"region"          yaml:"region,omitempty"`
	Profile       *string            `mapstructure:"profile"         yaml:"profile,omitempty"`
	AssumeRoleARN *string            `mapstructure:"assume_role_arn" yaml:"assume_role_arn,omitempty"`
	WebIdentity   *WebIdentityConfig `mapstructure:"web_identity"    yaml:"web_identity,omitempty"`

	// oauth
	TokenEndpoint   string `mapstructure:"token_endpoint"    yaml:"token_endpoint,omitempty"`
	ClientIDEnv     string `mapstructure:"client_id_env"     yaml:"client_id_env,omitempty"`
	ClientSecretEnv string `mapstructure:"client_secret_env" yaml:"client_secret_env,omitempty"`
	Scope           string `mapstructure:"scope"             yaml:"scope,omitempty"`
}

// TLSConfig configures broker-side and client-side TLS material.
type TLSConfig struct {
	Enabled            bool   `mapstructure:"enabled"              yaml:"enabled"`
	CAFile             string `mapstructure:"ca_file"              yaml:"ca_file,omitempty"`
	CertFile           string `mapstructure:"cert_file"            yaml:"cert_file,omitempty"`
	KeyFile            string `mapstructure:"key_file"             yaml:"key_file,omitempty"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify" yaml:"insecure_skip_verify,omitempty"`
	ServerName         string `mapstructure:"server_name"          yaml:"server_name,omitempty"`
}

// WebIdentityConfig configures EKS IRSA / OIDC token-file based assume role.
type WebIdentityConfig struct {
	RoleARN         string `mapstructure:"role_arn"          yaml:"role_arn"`
	TokenFile       string `mapstructure:"token_file"        yaml:"token_file"`
	RoleSessionName string `mapstructure:"role_session_name" yaml:"role_session_name,omitempty"`
}

// SchemaRegistryConfig — optional Confluent SR integration.
type SchemaRegistryConfig struct {
	Provider        string       `mapstructure:"provider"         yaml:"provider"`
	URL             string       `mapstructure:"url"              yaml:"url"`
	Auth            SRAuthConfig `mapstructure:"auth"             yaml:"auth"`
	SubjectStrategy string       `mapstructure:"subject_strategy" yaml:"subject_strategy"`
	OnFailure       string       `mapstructure:"on_failure"       yaml:"on_failure"`
}

// SRAuthConfig — basic or bearer auth on top of the SR HTTP endpoint.
type SRAuthConfig struct {
	Type        string `mapstructure:"type"         yaml:"type"`
	UsernameEnv string `mapstructure:"username_env" yaml:"username_env,omitempty"`
	PasswordEnv string `mapstructure:"password_env" yaml:"password_env,omitempty"`
	TokenEnv    string `mapstructure:"token_env"    yaml:"token_env,omitempty"`
}

// OwnersConfig — sources and precedence for owner resolution.
type OwnersConfig struct {
	Precedence  []string               `mapstructure:"precedence"   yaml:"precedence"`
	File        *OwnersFileConfig      `mapstructure:"file"         yaml:"file,omitempty"`
	TopicConfig *OwnersTopicConfig     `mapstructure:"topic_config" yaml:"topic_config,omitempty"`
	Backstage   *OwnersBackstageConfig `mapstructure:"backstage"    yaml:"backstage,omitempty"`
	JSON        *OwnersJSONConfig      `mapstructure:"json"         yaml:"json,omitempty"`
}

// OwnersFileConfig configures a local YAML/JSON file owner source.
type OwnersFileConfig struct {
	Path string `mapstructure:"path" yaml:"path"`
}

// OwnersTopicConfig configures owner extraction from a Kafka topic config key.
type OwnersTopicConfig struct {
	Key string `mapstructure:"key" yaml:"key"`
}

// OwnersBackstageConfig configures the Backstage catalog owner source.
type OwnersBackstageConfig struct {
	URL              string       `mapstructure:"url"               yaml:"url"`
	Auth             SRAuthConfig `mapstructure:"auth"              yaml:"auth"`
	EntityPattern    string       `mapstructure:"entity_pattern"    yaml:"entity_pattern"`
	FallbackRelation string       `mapstructure:"fallback_relation" yaml:"fallback_relation"`
}

// OwnersJSONConfig configures a generic JSON HTTP owner source.
type OwnersJSONConfig struct {
	URL     string            `mapstructure:"url"     yaml:"url"`
	Headers map[string]string `mapstructure:"headers" yaml:"headers"`
	Extract string            `mapstructure:"extract" yaml:"extract"`
}

// AtticScoreConfig — weights, thresholds and the piecewise activity curve.
type AtticScoreConfig struct {
	SpecVersion   string               `mapstructure:"spec_version"   yaml:"spec_version"`
	Weights       Weights              `mapstructure:"weights"        yaml:"weights"`
	Thresholds    Thresholds           `mapstructure:"thresholds"     yaml:"thresholds"`
	ActivityCurve []ActivityCurvePoint `mapstructure:"activity_curve" yaml:"activity_curve"`
}

// Weights — must sum to 1.0 within tolerance.
type Weights struct {
	Activity    float64 `mapstructure:"activity"    yaml:"activity"`
	Tenancy     float64 `mapstructure:"tenancy"     yaml:"tenancy"`
	Tonnage     float64 `mapstructure:"tonnage"     yaml:"tonnage"`
	Intent      float64 `mapstructure:"intent"      yaml:"intent"`
	Consumption float64 `mapstructure:"consumption" yaml:"consumption"`
}

// Sum returns the total weight; useful for validation and redistribution.
func (w Weights) Sum() float64 {
	return w.Activity + w.Tenancy + w.Tonnage + w.Intent + w.Consumption
}

// Thresholds — must satisfy likely_unused > candidate > inspect.
type Thresholds struct {
	LikelyUnused int `mapstructure:"likely_unused" yaml:"likely_unused"`
	Candidate    int `mapstructure:"candidate"     yaml:"candidate"`
	Inspect      int `mapstructure:"inspect"       yaml:"inspect"`
}

// ActivityCurvePoint — one point on the piecewise-linear days → sub-score curve.
type ActivityCurvePoint struct {
	Days  int     `mapstructure:"days"  yaml:"days"`
	Score float64 `mapstructure:"score" yaml:"score"`
}

// OversizedConfig governs the OVERSIZED flag detector.
type OversizedConfig struct {
	MaxPartitionsForThroughput int  `mapstructure:"max_partitions_for_throughput" yaml:"max_partitions_for_throughput"`
	LowTrafficMsgsPerSec       int  `mapstructure:"low_traffic_msgs_per_sec"      yaml:"low_traffic_msgs_per_sec"`
	RequiresMetrics            bool `mapstructure:"requires_metrics"              yaml:"requires_metrics"`
}

// SkewConfig governs the SKEWED flag detector.
type SkewConfig struct {
	MaxRatioToAverage float64 `mapstructure:"max_ratio_to_average" yaml:"max_ratio_to_average"`
}

// MetricsConfig selects the optional external metrics source.
type MetricsConfig struct {
	Source     string                   `mapstructure:"source"     yaml:"source"`
	Prometheus *MetricsPrometheusConfig `mapstructure:"prometheus" yaml:"prometheus,omitempty"`
}

// MetricsPrometheusConfig configures the Prometheus metrics source.
type MetricsPrometheusConfig struct {
	URL             string `mapstructure:"url"                yaml:"url"`
	QueryMsgsPerSec string `mapstructure:"query_msgs_per_sec" yaml:"query_msgs_per_sec"`
}

// HistoryConfig configures the local SQLite history store.
type HistoryConfig struct {
	Enabled       bool   `mapstructure:"enabled"        yaml:"enabled"`
	Path          string `mapstructure:"path"           yaml:"path"`
	RetentionDays int    `mapstructure:"retention_days" yaml:"retention_days"`
}

// ExcludePatternsConfig configures topic-name exclusion patterns.
type ExcludePatternsConfig struct {
	Defaults   bool     `mapstructure:"defaults"   yaml:"defaults"`
	Effect     string   `mapstructure:"effect"     yaml:"effect"`
	Additional []string `mapstructure:"additional" yaml:"additional"`
}

// TelemetryConfig configures anonymous telemetry pings.
type TelemetryConfig struct {
	Enabled                 bool   `mapstructure:"enabled"                    yaml:"enabled"`
	Endpoint                string `mapstructure:"endpoint"                   yaml:"endpoint"`
	IncludeAnonymousRunUUID bool   `mapstructure:"include_anonymous_run_uuid" yaml:"include_anonymous_run_uuid"`
}

// ReportConfig configures report rendering and output.
type ReportConfig struct {
	Format               string            `mapstructure:"format"                 yaml:"format"`
	Output               string            `mapstructure:"output"                 yaml:"output"`
	IncludeCleanupScript bool              `mapstructure:"include_cleanup_script" yaml:"include_cleanup_script"`
	RedactTopicNames     string            `mapstructure:"redact_topic_names"     yaml:"redact_topic_names"`
	UTM                  map[string]string `mapstructure:"utm"                    yaml:"utm"`
}
