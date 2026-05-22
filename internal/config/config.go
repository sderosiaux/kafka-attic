// Package config loads, validates, and represents kattic.yaml configuration.
package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// weightTolerance is the +/- allowed drift around 1.0 when summing weights.
// 1e-6 absorbs float64 representation noise while still rejecting human errors
// like 0.3 + 0.2 + 0.1 + 0.1 + 0.25 = 0.95.
const weightTolerance = 1e-6

// Load reads a kattic.yaml file from disk, applies environment-variable
// interpolation to known *_env fields, validates structural invariants, and
// returns a fully-typed *Config. The path must be non-empty.
func Load(path string) (*Config, error) {
	if path == "" {
		return nil, errors.New("config path is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve config path: %w", err)
	}

	v := viper.New()
	v.SetConfigFile(abs)
	v.SetConfigType("yaml")
	rerr := v.ReadInConfig()
	if rerr != nil {
		return nil, fmt.Errorf("read config %s: %w", abs, rerr)
	}

	var cfg Config
	uerr := v.Unmarshal(&cfg)
	if uerr != nil {
		return nil, fmt.Errorf("unmarshal config: %w", uerr)
	}

	verr := Validate(&cfg)
	if verr != nil {
		return nil, verr
	}

	return &cfg, nil
}

// Validate enforces the invariants documented in SPEC §4 and Appendix B that
// must hold before we touch a cluster.
func Validate(cfg *Config) error {
	if cfg == nil {
		return errors.New("config is nil")
	}

	if strings.TrimSpace(cfg.Cluster.Bootstrap) == "" {
		return errors.New("cluster.bootstrap is required")
	}
	if cfg.Cluster.Name == "" {
		return errors.New("cluster.name is required")
	}
	aerr := validateAuth(cfg.Cluster.Auth)
	if aerr != nil {
		return fmt.Errorf("cluster.auth: %w", aerr)
	}

	werr := validateWeights(cfg.AtticScore.Weights)
	if werr != nil {
		return werr
	}
	terr := validateThresholds(cfg.AtticScore.Thresholds)
	if terr != nil {
		return terr
	}
	return validateActivityCurve(cfg.AtticScore.ActivityCurve)
}

func validateAuth(a AuthConfig) error {
	switch a.Type {
	case AuthNone:
		// Plaintext / no-auth broker. Intended for local development and
		// integration tests against a no-SASL broker. Production deployments
		// should always specify a real auth type.
		return nil
	case AuthSASLPlain:
		return validateUserPassEnv(a, "sasl_plain")
	case AuthSCRAM:
		return validateSCRAM(a)
	case AuthMTLS:
		return validateMTLS(a)
	case AuthIAM:
		if strings.TrimSpace(a.Region) == "" {
			return errors.New("iam requires region")
		}
		return nil
	case AuthOAuth:
		return validateOAuth(a)
	default:
		return fmt.Errorf("unknown auth.type %q (want one of: sasl_plain, scram, mtls, iam, oauth)", a.Type)
	}
}

func validateUserPassEnv(a AuthConfig, label string) error {
	if a.UsernameEnv == "" || a.PasswordEnv == "" {
		return fmt.Errorf("%s requires username_env and password_env", label)
	}
	if _, ok := os.LookupEnv(a.UsernameEnv); !ok {
		return fmt.Errorf("env var %q is not set", a.UsernameEnv)
	}
	if _, ok := os.LookupEnv(a.PasswordEnv); !ok {
		return fmt.Errorf("env var %q is not set", a.PasswordEnv)
	}
	return nil
}

func validateSCRAM(a AuthConfig) error {
	mech := strings.ToUpper(strings.TrimSpace(a.Mechanism))
	if mech != "SCRAM-SHA-256" && mech != "SCRAM-SHA-512" {
		return fmt.Errorf("scram mechanism must be SCRAM-SHA-256 or SCRAM-SHA-512, got %q", a.Mechanism)
	}
	return validateUserPassEnv(a, "scram")
}

func validateMTLS(a AuthConfig) error {
	if a.TLS == nil {
		return errors.New("mtls requires tls block with cert_file and key_file")
	}
	if a.TLS.CertFile == "" || a.TLS.KeyFile == "" {
		return errors.New("mtls requires tls.cert_file and tls.key_file")
	}
	return nil
}

func validateOAuth(a AuthConfig) error {
	if a.TokenEndpoint == "" {
		return errors.New("oauth requires token_endpoint")
	}
	if a.ClientIDEnv == "" || a.ClientSecretEnv == "" {
		return errors.New("oauth requires client_id_env and client_secret_env")
	}
	if _, ok := os.LookupEnv(a.ClientIDEnv); !ok {
		return fmt.Errorf("env var %q is not set", a.ClientIDEnv)
	}
	if _, ok := os.LookupEnv(a.ClientSecretEnv); !ok {
		return fmt.Errorf("env var %q is not set", a.ClientSecretEnv)
	}
	return nil
}

func validateWeights(w Weights) error {
	sum := w.Sum()
	if math.Abs(sum-1.0) > weightTolerance {
		return fmt.Errorf("attic_score.weights must sum to 1.0, got %.6f (activity=%.3f tenancy=%.3f tonnage=%.3f intent=%.3f consumption=%.3f)",
			sum, w.Activity, w.Tenancy, w.Tonnage, w.Intent, w.Consumption)
	}
	for name, v := range map[string]float64{
		"activity":    w.Activity,
		"tenancy":     w.Tenancy,
		"tonnage":     w.Tonnage,
		"intent":      w.Intent,
		"consumption": w.Consumption,
	} {
		if v < 0 {
			return fmt.Errorf("attic_score.weights.%s must be >= 0, got %.3f", name, v)
		}
	}
	return nil
}

func validateThresholds(t Thresholds) error {
	if t.LikelyUnused <= t.Candidate || t.Candidate <= t.Inspect {
		return fmt.Errorf("attic_score.thresholds must satisfy likely_unused > candidate > inspect, got likely_unused=%d candidate=%d inspect=%d",
			t.LikelyUnused, t.Candidate, t.Inspect)
	}
	if t.LikelyUnused > 100 || t.Inspect < 0 {
		return fmt.Errorf("attic_score.thresholds must be within [0,100], got likely_unused=%d inspect=%d", t.LikelyUnused, t.Inspect)
	}
	return nil
}

func validateActivityCurve(curve []ActivityCurvePoint) error {
	if len(curve) < 2 {
		return errors.New("attic_score.activity_curve must have at least two points")
	}
	for i := 1; i < len(curve); i++ {
		if curve[i].Days <= curve[i-1].Days {
			return fmt.Errorf("attic_score.activity_curve must be strictly increasing in days, point %d: %d <= %d",
				i, curve[i].Days, curve[i-1].Days)
		}
		if curve[i].Score < curve[i-1].Score {
			return fmt.Errorf("attic_score.activity_curve scores must be non-decreasing, point %d: %.1f < %.1f",
				i, curve[i].Score, curve[i-1].Score)
		}
	}
	return nil
}

// ResolveEnv returns the value of the named env var or "" when unset. Used by
// the cluster factory at connect time so that env values picked up after Load
// (e.g. by a wrapper script) still win.
func ResolveEnv(name string) string {
	if name == "" {
		return ""
	}
	return os.Getenv(name)
}
