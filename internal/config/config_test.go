package config

import (
	"strings"
	"testing"
)

func validWeights() Weights {
	return Weights{Activity: 0.30, Tenancy: 0.20, Tonnage: 0.10, Intent: 0.15, Consumption: 0.25}
}

func validThresholds() Thresholds {
	return Thresholds{LikelyUnused: 90, Candidate: 70, Inspect: 40}
}

func validCurve() []ActivityCurvePoint {
	return []ActivityCurvePoint{
		{Days: 0, Score: 0},
		{Days: 30, Score: 25},
		{Days: 90, Score: 60},
		{Days: 180, Score: 80},
		{Days: 365, Score: 100},
	}
}

func TestValidateWeightsSumOne(t *testing.T) {
	cases := []struct {
		name    string
		w       Weights
		wantErr string
	}{
		{"exact 1.0", validWeights(), ""},
		{"floats add to 1.0 within tol", Weights{0.1, 0.1, 0.1, 0.1, 0.6}, ""},
		{"under 1.0", Weights{0.1, 0.1, 0.1, 0.1, 0.5}, "must sum to 1.0"},
		{"over 1.0", Weights{0.3, 0.3, 0.2, 0.1, 0.2}, "must sum to 1.0"},
		{"negative weight", Weights{-0.1, 0.3, 0.2, 0.2, 0.4}, ""}, // sum is 1.0 so weights sum check passes; negative gate triggers
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateWeights(c.w)
			if c.wantErr == "" {
				if c.name == "negative weight" {
					if err == nil || !strings.Contains(err.Error(), "must be >= 0") {
						t.Fatalf("want negative-weight rejection, got %v", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want error containing %q, got %v", c.wantErr, err)
			}
		})
	}
}

func TestValidateThresholdsOrdering(t *testing.T) {
	cases := []struct {
		name    string
		th      Thresholds
		wantErr string
	}{
		{"valid", validThresholds(), ""},
		{"likely_unused not > candidate", Thresholds{70, 70, 40}, "likely_unused > candidate"},
		{"candidate not > inspect", Thresholds{90, 40, 40}, "candidate > inspect"},
		{"inverted", Thresholds{40, 70, 90}, "likely_unused > candidate"},
		{"out of range", Thresholds{200, 70, 40}, "within [0,100]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateThresholds(c.th)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want error containing %q, got %v", c.wantErr, err)
			}
		})
	}
}

func TestValidateActivityCurve(t *testing.T) {
	if err := validateActivityCurve(validCurve()); err != nil {
		t.Fatalf("valid curve rejected: %v", err)
	}
	if err := validateActivityCurve([]ActivityCurvePoint{{0, 0}}); err == nil {
		t.Fatal("single-point curve should be rejected")
	}
	if err := validateActivityCurve([]ActivityCurvePoint{{0, 0}, {0, 1}}); err == nil {
		t.Fatal("non-increasing days should be rejected")
	}
}

func TestValidateAuthRejectsUnknown(t *testing.T) {
	cfg := &Config{
		Cluster: ClusterConfig{
			Name:      "x",
			Bootstrap: "h:9092",
			Auth:      AuthConfig{Type: AuthType("nope")},
		},
		AtticScore: AtticScoreConfig{
			Weights:       validWeights(),
			Thresholds:    validThresholds(),
			ActivityCurve: validCurve(),
		},
	}
	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "unknown auth.type") {
		t.Fatalf("want unknown auth.type, got %v", err)
	}
}

func TestValidateMTLSRequiresCertAndKey(t *testing.T) {
	cfg := &Config{
		Cluster: ClusterConfig{
			Name:      "x",
			Bootstrap: "h:9092",
			Auth:      AuthConfig{Type: AuthMTLS, TLS: &TLSConfig{Enabled: true}},
		},
		AtticScore: AtticScoreConfig{
			Weights:       validWeights(),
			Thresholds:    validThresholds(),
			ActivityCurve: validCurve(),
		},
	}
	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "cert_file") {
		t.Fatalf("want cert_file error, got %v", err)
	}
}

func TestValidateIAMRequiresRegion(t *testing.T) {
	cfg := &Config{
		Cluster: ClusterConfig{
			Name:      "x",
			Bootstrap: "h:9098",
			Auth:      AuthConfig{Type: AuthIAM},
		},
		AtticScore: AtticScoreConfig{
			Weights:       validWeights(),
			Thresholds:    validThresholds(),
			ActivityCurve: validCurve(),
		},
	}
	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "region") {
		t.Fatalf("want region error, got %v", err)
	}
}
