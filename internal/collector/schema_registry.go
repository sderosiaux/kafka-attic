package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/sderosiaux/kafka-attic/internal/config"
	"github.com/sderosiaux/kafka-attic/internal/types"
)

// SRClient is the read-only Confluent Schema Registry client interface.
// Production wires it to a real HTTP client; tests use a fake.
type SRClient interface {
	// ListSubjects returns the names of every subject the registry exposes,
	// or an error. Implementations must not block longer than the configured
	// request timeout.
	ListSubjects(ctx context.Context) ([]string, error)
}

// httpSRClient is the production HTTP implementation. It speaks the Confluent
// Schema Registry REST API: GET /subjects → JSON array of strings.
type httpSRClient struct {
	baseURL    string
	httpClient *http.Client
	auth       config.SRAuthConfig
}

// NewSRClient builds a Confluent SR client from cfg. Returns nil + nil error
// when SR is not configured — call-sites must check for that case.
func NewSRClient(cfg *config.Config) (SRClient, error) {
	if cfg == nil || cfg.SchemaRegistry == nil {
		return nil, nil //nolint:nilnil // documented contract: nil client means SR not configured
	}
	if cfg.SchemaRegistry.Provider != "" && cfg.SchemaRegistry.Provider != "confluent" {
		// v1 supports only Confluent SR. Other providers (Glue, Apicurio) are
		// v1.1 per SPEC §2 Out of scope.
		return nil, fmt.Errorf("schema_registry.provider %q not supported in v1 (confluent only)", cfg.SchemaRegistry.Provider)
	}
	base := strings.TrimRight(cfg.SchemaRegistry.URL, "/")
	if base == "" {
		return nil, errors.New("schema_registry.url is required when schema_registry block is present")
	}
	return &httpSRClient{
		baseURL:    base,
		httpClient: &http.Client{Timeout: 15 * time.Second},
		auth:       cfg.SchemaRegistry.Auth,
	}, nil
}

// ListSubjects fetches /subjects from the configured registry.
func (c *httpSRClient) ListSubjects(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/subjects", http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("build SR request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")

	switch strings.ToLower(c.auth.Type) {
	case "basic":
		user := config.ResolveEnv(c.auth.UsernameEnv)
		pass := config.ResolveEnv(c.auth.PasswordEnv)
		if user != "" || pass != "" {
			req.SetBasicAuth(user, pass)
		}
	case "bearer":
		tok := config.ResolveEnv(c.auth.TokenEnv)
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	case "", "none":
		// nothing.
	default:
		return nil, fmt.Errorf("unsupported schema_registry.auth.type %q", c.auth.Type)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("SR /subjects: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("SR /subjects status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var subs []string
	derr := json.NewDecoder(resp.Body).Decode(&subs)
	if derr != nil {
		return nil, fmt.Errorf("decode SR subjects: %w", derr)
	}
	return subs, nil
}

// srResult carries the per-topic SR view back to the orchestrator.
//
// SubjectStrategy is the strategy from cfg, mirrored into every per-topic
// info block for snapshot completeness. Strategy `record_name` cannot be
// resolved from SR alone (SPEC §4.2 Intent table) — the collector marks every
// in-scope topic as "skipped" so the scorer redistributes the Intent weight.
type srResult struct {
	// PerTopic maps topic → SchemaRegistryInfo. A topic missing from the
	// map means SR is not configured at all.
	PerTopic map[string]*types.SchemaRegistryInfo

	// Configured tells the orchestrator whether to set
	// permissions_observed.schema_registry_read = true.
	Configured bool

	// Reachable is true when ListSubjects succeeded. False = SR was either
	// configured-but-unreachable (Intent skipped) or strategy is record_name
	// (also skipped).
	Reachable bool

	// Skipped is true for every topic in PerTopic when the strategy is
	// `record_name`. The scorer reads it via SchemaRegistryInfo.Evidence ==
	// UNKNOWN combined with the strategy name; we keep an explicit boolean
	// here so the orchestrator can avoid emitting ORPHAN_SCHEMA on a
	// strategy-skip.
	SkippedStrategy bool
}

// collectSchemaRegistry fetches /subjects (when configured) and computes,
// per topic, whether a matching subject exists under the configured naming
// strategy.
func collectSchemaRegistry(
	ctx context.Context,
	cli SRClient,
	cfg *config.Config,
	topics []string,
) *srResult {
	res := &srResult{PerTopic: map[string]*types.SchemaRegistryInfo{}}
	if cfg == nil || cfg.SchemaRegistry == nil {
		return res
	}
	res.Configured = true
	strategy := strings.TrimSpace(cfg.SchemaRegistry.SubjectStrategy)
	if strategy == "" {
		strategy = "topic_name"
	}

	// record_name strategy is skipped per SPEC §4.2.
	if strategy == "record_name" {
		res.SkippedStrategy = true
		for _, t := range topics {
			res.PerTopic[t] = &types.SchemaRegistryInfo{
				SubjectStrategy: strategy,
				SubjectsFound:   []string{},
				Evidence:        types.EvidenceUnknown,
			}
		}
		return res
	}

	if cli == nil {
		// Configured but no client. Treat as unreachable.
		for _, t := range topics {
			res.PerTopic[t] = &types.SchemaRegistryInfo{
				SubjectStrategy: strategy,
				SubjectsFound:   []string{},
				Evidence:        types.EvidenceUnknown,
			}
		}
		return res
	}

	subs, err := cli.ListSubjects(ctx)
	if err != nil {
		// on_failure: warn = degrade silently; on_failure: fail = SPEC says
		// abort. v1 honors `warn` (the default in Appendix B) by treating
		// every topic's Intent as UNKNOWN; the scorer will then skip it.
		for _, t := range topics {
			res.PerTopic[t] = &types.SchemaRegistryInfo{
				SubjectStrategy: strategy,
				SubjectsFound:   []string{},
				Evidence:        types.EvidenceUnknown,
			}
		}
		return res
	}
	res.Reachable = true

	// Index subjects to make per-topic lookups O(1) for topic_name and
	// O(s/topic) for topic_record. For topic_record we precompute the
	// per-prefix index so each topic only walks the subjects with the right
	// prefix.
	indexByName := make(map[string]struct{}, len(subs))
	for _, s := range subs {
		indexByName[s] = struct{}{}
	}

	for _, t := range topics {
		var found []string
		switch strategy {
		case "topic_name":
			// Confluent's TopicNameStrategy: `<topic>-key` and `<topic>-value`.
			for _, suffix := range []string{"-key", "-value"} {
				name := t + suffix
				if _, ok := indexByName[name]; ok {
					found = append(found, name)
				}
			}
		case "topic_record":
			// Confluent's TopicRecordNameStrategy: `<topic>-<recordName>`.
			// We accept any subject prefixed with `<topic>-`.
			prefix := t + "-"
			for _, s := range subs {
				if strings.HasPrefix(s, prefix) {
					found = append(found, s)
				}
			}
		default:
			// Unknown strategy: same treatment as record_name.
			res.PerTopic[t] = &types.SchemaRegistryInfo{
				SubjectStrategy: strategy,
				SubjectsFound:   []string{},
				Evidence:        types.EvidenceUnknown,
			}
			continue
		}
		sort.Strings(found)
		res.PerTopic[t] = &types.SchemaRegistryInfo{
			SubjectStrategy: strategy,
			SubjectsFound:   found,
			Evidence:        types.EvidenceKnown,
		}
	}
	return res
}
