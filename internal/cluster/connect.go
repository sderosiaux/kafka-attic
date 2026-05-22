// Package cluster builds franz-go kgo.Client configurations from kafka-attic
// cluster + auth configuration, including TLS, SASL, and OAuth.
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/sderosiaux/kafka-attic/internal/config"
)

// Clients bundles the two franz-go handles a read-only collector needs. The
// kgo client is wrapped so any future contributor wiring a producer would
// have to either bypass this type or change the constructor signature — both
// of which the readonly_test.go static scan catches.
type Clients struct {
	Kgo  *kgo.Client
	Kadm *kadm.Client
}

// Close releases both handles. Safe to call on a zero value.
func (c *Clients) Close() {
	if c == nil {
		return
	}
	if c.Kadm != nil {
		c.Kadm.Close()
	}
	if c.Kgo != nil {
		c.Kgo.Close()
	}
}

// Connect builds a read-only Kafka client pair from a fully-validated config.
// The returned kgo.Client is intentionally configured WITHOUT producer
// options; callers must never invoke Produce* on it (enforced by
// readonly_test.go's source scan).
func Connect(ctx context.Context, cfg *config.Config) (*Clients, error) {
	if cfg == nil {
		return nil, errors.New("cluster.Connect: nil config")
	}
	opts, err := clientOptions(ctx, cfg)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kgo client: %w", err)
	}
	adm := kadm.NewClient(cl)
	return &Clients{Kgo: cl, Kadm: adm}, nil
}

// clientOptions assembles the kgo functional options for cfg. Extracted so
// tests can inspect the assembled option set without dialing a real broker.
func clientOptions(ctx context.Context, cfg *config.Config) ([]kgo.Opt, error) {
	seeds := splitBootstrap(cfg.Cluster.Bootstrap)
	if len(seeds) == 0 {
		return nil, errors.New("cluster.bootstrap yielded no seed brokers")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ClientID("kafka-attic"),
		// Hard cap so a slow broker never wedges the CLI for minutes.
		kgo.RequestTimeoutOverhead(15 * time.Second),
	}

	tlsCfg, useTLS, err := buildTLS(cfg.Cluster.Auth)
	if err != nil {
		return nil, err
	}

	switch cfg.Cluster.Auth.Type {
	case config.AuthNone:
		// No SASL mechanism attached; broker is plaintext or TLS-only.
	case config.AuthSASLPlain:
		user := config.ResolveEnv(cfg.Cluster.Auth.UsernameEnv)
		pass := config.ResolveEnv(cfg.Cluster.Auth.PasswordEnv)
		if user == "" || pass == "" {
			return nil, fmt.Errorf("sasl_plain credentials empty (username_env=%s password_env=%s)",
				cfg.Cluster.Auth.UsernameEnv, cfg.Cluster.Auth.PasswordEnv)
		}
		opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()))

	case config.AuthSCRAM:
		user := config.ResolveEnv(cfg.Cluster.Auth.UsernameEnv)
		pass := config.ResolveEnv(cfg.Cluster.Auth.PasswordEnv)
		if user == "" || pass == "" {
			return nil, fmt.Errorf("scram credentials empty (username_env=%s password_env=%s)",
				cfg.Cluster.Auth.UsernameEnv, cfg.Cluster.Auth.PasswordEnv)
		}
		mech := strings.ToUpper(strings.TrimSpace(cfg.Cluster.Auth.Mechanism))
		switch mech {
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("unsupported scram mechanism %q", cfg.Cluster.Auth.Mechanism)
		}

	case config.AuthMTLS:
		// TLS already built; nothing more to do here. The client cert is
		// the credential.
		if tlsCfg == nil {
			return nil, errors.New("mtls requires tls configuration with cert_file and key_file")
		}

	case config.AuthIAM:
		mech, err := buildIAMMechanism(ctx, cfg.Cluster.Auth)
		if err != nil {
			return nil, fmt.Errorf("build IAM SASL: %w", err)
		}
		opts = append(opts, kgo.SASL(mech))
		// MSK IAM always rides on TLS; force it on even if the user
		// forgot the explicit tls block.
		if tlsCfg == nil {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
			useTLS = true
		}

	case config.AuthOAuth:
		mech, err := buildOAuthMechanism(cfg.Cluster.Auth)
		if err != nil {
			return nil, fmt.Errorf("build OAUTHBEARER SASL: %w", err)
		}
		opts = append(opts, kgo.SASL(mech))

	default:
		return nil, fmt.Errorf("unsupported auth.type %q", cfg.Cluster.Auth.Type)
	}

	if useTLS {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	return opts, nil
}

// splitBootstrap accepts comma- or whitespace-separated bootstrap strings.
func splitBootstrap(s string) []string {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

// buildTLS returns (tlsConfig, useTLS, error). useTLS=true means callers
// should attach kgo.DialTLSConfig. tlsConfig is nil-safe: a non-mTLS, non-IAM
// caller with no tls block gets (nil, false, nil).
func buildTLS(a config.AuthConfig) (*tls.Config, bool, error) {
	// mTLS always needs TLS.
	mustTLS := a.Type == config.AuthMTLS || a.Type == config.AuthIAM
	if a.TLS == nil && !mustTLS {
		return nil, false, nil
	}

	t := &tls.Config{MinVersion: tls.VersionTLS12}
	if a.TLS == nil {
		return t, true, nil
	}
	err := applyTLSConfig(t, a.TLS)
	if err != nil {
		return nil, false, err
	}
	return t, true, nil
}

// applyTLSConfig mutates t with options drawn from cfg (CA bundle, client
// cert/key, ServerName, InsecureSkipVerify).
func applyTLSConfig(t *tls.Config, cfg *config.TLSConfig) error {
	t.InsecureSkipVerify = cfg.InsecureSkipVerify
	if cfg.ServerName != "" {
		t.ServerName = cfg.ServerName
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return fmt.Errorf("read ca_file %s: %w", cfg.CAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return fmt.Errorf("ca_file %s contains no usable certificates", cfg.CAFile)
		}
		t.RootCAs = pool
	}
	if cfg.CertFile == "" && cfg.KeyFile == "" {
		return nil
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return errors.New("tls.cert_file and tls.key_file must both be set")
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return fmt.Errorf("load client cert/key: %w", err)
	}
	t.Certificates = []tls.Certificate{cert}
	return nil
}

// buildIAMMechanism wires franz-go's aws.ManagedStreamingIAM to a
// credentials provider chain that honors, in order:
//   - explicit assume_role_arn (with optional STS web-identity inner chain)
//   - explicit profile (config.WithSharedConfigProfile)
//   - default chain (env, ECS, EC2, AWS_PROFILE, IRSA via env)
//
// The callback re-resolves credentials on every connect attempt so short-
// lived STS sessions don't expire silently.
func buildIAMMechanism(ctx context.Context, a config.AuthConfig) (sasl.Mechanism, error) {
	provider, region, err := resolveAWSProvider(ctx, a)
	if err != nil {
		return nil, err
	}
	return aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
		creds, err := provider.Retrieve(ctx)
		if err != nil {
			return aws.Auth{}, fmt.Errorf("retrieve AWS credentials: %w", err)
		}
		return aws.Auth{
			AccessKey:    creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
			UserAgent:    "kafka-attic/0.0 region=" + region,
		}, nil
	}), nil
}

// resolveAWSProvider returns a credentials provider plus the active region.
// The provider may be a default chain, a profile-scoped chain, an
// AssumeRole-wrapped chain, or a WebIdentity-wrapped chain — depending on
// which knobs the user set in kattic.yaml.
func resolveAWSProvider(ctx context.Context, a config.AuthConfig) (awsv2.CredentialsProvider, string, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(a.Region),
	}
	if a.Profile != nil && *a.Profile != "" {
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(*a.Profile))
	}

	base, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, "", fmt.Errorf("load AWS config: %w", err)
	}

	region := base.Region
	if region == "" {
		region = a.Region
	}

	// Web identity (EKS IRSA via explicit token file).
	if a.WebIdentity != nil && a.WebIdentity.RoleARN != "" {
		stsClient := sts.NewFromConfig(base)
		sessionName := a.WebIdentity.RoleSessionName
		if sessionName == "" {
			sessionName = "kafka-attic"
		}
		provider := stscreds.NewWebIdentityRoleProvider(
			stsClient,
			a.WebIdentity.RoleARN,
			stscreds.IdentityTokenFile(a.WebIdentity.TokenFile),
			func(o *stscreds.WebIdentityRoleOptions) {
				o.RoleSessionName = sessionName
			},
		)
		// If assume_role_arn is also set, chain WebIdentity → AssumeRole.
		if a.AssumeRoleARN != nil && *a.AssumeRoleARN != "" {
			inner := base.Copy()
			inner.Credentials = awsv2.NewCredentialsCache(provider)
			outerSTS := sts.NewFromConfig(inner)
			return awsv2.NewCredentialsCache(stscreds.NewAssumeRoleProvider(outerSTS, *a.AssumeRoleARN)), region, nil
		}
		return awsv2.NewCredentialsCache(provider), region, nil
	}

	// Explicit assume_role_arn on top of the resolved base chain.
	if a.AssumeRoleARN != nil && *a.AssumeRoleARN != "" {
		stsClient := sts.NewFromConfig(base)
		return awsv2.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient, *a.AssumeRoleARN)), region, nil
	}

	return base.Credentials, region, nil
}

// buildOAuthMechanism wires OAUTHBEARER using a simple client-credentials
// grant against TokenEndpoint. The mechanism callback re-mints tokens before
// expiry; franz-go re-invokes it as needed.
func buildOAuthMechanism(a config.AuthConfig) (sasl.Mechanism, error) {
	clientID := config.ResolveEnv(a.ClientIDEnv)
	clientSecret := config.ResolveEnv(a.ClientSecretEnv)
	if clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("oauth credentials empty (client_id_env=%s client_secret_env=%s)",
			a.ClientIDEnv, a.ClientSecretEnv)
	}
	endpoint := a.TokenEndpoint
	scope := a.Scope

	httpClient := &http.Client{Timeout: 10 * time.Second}

	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		token, _, err := fetchOAuthToken(ctx, httpClient, endpoint, clientID, clientSecret, scope)
		if err != nil {
			return oauth.Auth{}, err
		}
		return oauth.Auth{Token: token}, nil
	}), nil
}

// fetchOAuthToken posts a client_credentials grant and parses the access
// token + expiry. Deliberately minimal: kafka-attic does not need refresh
// tokens, PKCE, or device flow for v1.
func fetchOAuthToken(ctx context.Context, httpClient *http.Client, endpoint, clientID, clientSecret, scope string) (string, time.Time, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)
	if scope != "" {
		form.Set("scope", scope)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("oauth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("oauth token endpoint: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", time.Time{}, fmt.Errorf("oauth token endpoint returned status %s", resp.Status)
	}
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	derr := json.NewDecoder(resp.Body).Decode(&body)
	if derr != nil {
		return "", time.Time{}, fmt.Errorf("oauth token decode: %w", derr)
	}
	if body.AccessToken == "" {
		return "", time.Time{}, errors.New("oauth token endpoint returned empty access_token")
	}
	ttl := time.Duration(body.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = time.Minute
	}
	return body.AccessToken, time.Now().Add(ttl), nil
}
