package collector

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/conduktor/kafka-attic/internal/config"
)

// defaultExcludePatterns mirrors the comment block in SPEC Appendix B
// (`exclude_patterns.defaults` reference list). They are applied when
// `exclude_patterns.defaults: true` (or when the user has provided no
// exclude_patterns block at all — we default to applying them).
var defaultExcludePatterns = []string{
	`^__.*`,
	`^_schemas$`,
	`.*\.dlq$`,
	`.*-changelog$`,
	`.*-repartition$`,
	`^mm2-.*`,
	`.*\.replica$`,
}

// topicConfigKeys is the list of topic-level config keys the collector pulls.
// Anything outside this list is ignored to keep DescribeConfigs payloads
// small. The optional owner key is appended at runtime when configured.
var topicConfigKeys = []string{
	"cleanup.policy",
	"retention.ms",
	"message.timestamp.type",
	"remote.storage.enable",
	"confluent.placement.constraints",
	"confluent.tier.enable",
}

// compileExcludeFilter resolves the exclude patterns from cfg into a single
// matcher func. Invalid regexes are logged via the returned warning list and
// skipped (per SPEC §5.4 spirit: never abort the scan for a config typo).
func compileExcludeFilter(cfg *config.Config) (func(string) bool, []string, error) {
	var pats []string
	applyDefaults := true
	if cfg.ExcludePatterns != nil {
		applyDefaults = cfg.ExcludePatterns.Defaults
		pats = append(pats, cfg.ExcludePatterns.Additional...)
	}
	if applyDefaults {
		pats = append(defaultExcludePatterns, pats...)
	}

	var warnings []string
	compiled := make([]*regexp.Regexp, 0, len(pats))
	for _, p := range pats {
		re, err := regexp.Compile(p)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("exclude_pattern %q invalid: %v", p, err))
			continue
		}
		compiled = append(compiled, re)
	}

	matcher := func(name string) bool {
		for _, re := range compiled {
			if re.MatchString(name) {
				return true
			}
		}
		return false
	}
	return matcher, warnings, nil
}

// listTopicsResult bundles the metadata and config outputs of phase 1.
type listTopicsResult struct {
	// inScope is the deterministic, sorted slice of topics that survived the
	// exclude filter. Order matches the order in Snapshot.Topics.
	inScope []string

	// metadata holds the kadm metadata for the in-scope topics.
	metadata kadm.TopicDetails

	// configs maps topic name → key → value (string). Missing keys are absent
	// from the inner map (not stored as empty strings) so call-sites can
	// distinguish "broker did not return" from "broker returned empty".
	configs map[string]map[string]string

	// excludedCount is the number of topics from the cluster metadata that
	// matched an exclude pattern. Surfaced in Snapshot.Scan.
	excludedCount int

	// configsAuthErr is true when DescribeTopicConfigs failed due to ACLs.
	// The scan continues; topic configs are treated as absent.
	configsAuthErr bool

	// warnings collected during pattern compilation, preserved for the
	// scan-level missing_signals_global list.
	warnings []string
}

// listTopicsAndConfigs lists every topic, filters by exclude_patterns, and
// fetches the relevant topic configs (cleanup.policy, retention.ms,
// remote.storage.enable, message.timestamp.type + optional owner key).
//
// The function never aborts on an auth error from DescribeTopicConfigs — it
// records the fact in the result and lets downstream stages degrade the
// per-topic evidence accordingly.
func listTopicsAndConfigs(ctx context.Context, adm KafkaAdmin, cfg *config.Config) (*listTopicsResult, error) {
	matcher, warnings, err := compileExcludeFilter(cfg)
	if err != nil {
		return nil, err
	}

	md, err := adm.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	// Drop internal topics (`__consumer_offsets`, `__transaction_state`, …).
	// Metadata returns them; we already exclude them via the `^__.*` default
	// pattern but FilterInternal is the explicit safety net.
	md.Topics.FilterInternal()

	excluded := 0
	inScope := make([]string, 0, len(md.Topics))
	keep := make(kadm.TopicDetails, len(md.Topics))
	for name, td := range md.Topics {
		if matcher(name) {
			excluded++
			continue
		}
		inScope = append(inScope, name)
		keep[name] = td
	}
	sort.Strings(inScope)

	res := &listTopicsResult{
		inScope:       inScope,
		metadata:      keep,
		configs:       make(map[string]map[string]string, len(inScope)),
		excludedCount: excluded,
		warnings:      warnings,
	}

	if len(inScope) == 0 {
		return res, nil
	}

	confs, cerr := adm.DescribeTopicConfigs(ctx, inScope...)
	if cerr != nil {
		// Authoritative auth errors arrive as *kadm.AuthError; we treat any
		// error here as "configs unavailable" and continue. The downstream
		// stages decide how much that degrades evidence.
		res.configsAuthErr = true
		return res, nil //nolint:nilerr // intentional: configs unavailable degrades evidence but never fails the scan
	}

	wantOwnerKey := ""
	if cfg.Owners != nil && cfg.Owners.TopicConfig != nil {
		wantOwnerKey = strings.TrimSpace(cfg.Owners.TopicConfig.Key)
	}

	wantKeys := make(map[string]struct{}, len(topicConfigKeys)+1)
	for _, k := range topicConfigKeys {
		wantKeys[k] = struct{}{}
	}
	if wantOwnerKey != "" {
		wantKeys[wantOwnerKey] = struct{}{}
	}

	for _, rc := range confs {
		if rc.Err != nil {
			// Per-topic failure (e.g. unknown topic between list and describe);
			// leave configs absent.
			continue
		}
		row := make(map[string]string, len(wantKeys))
		for _, c := range rc.Configs {
			if _, ok := wantKeys[c.Key]; !ok {
				continue
			}
			if c.Value == nil {
				continue
			}
			row[c.Key] = *c.Value
		}
		res.configs[rc.Name] = row
	}
	return res, nil
}

// configString returns the topic config value or the fallback when absent.
func configString(configs map[string]string, key, fallback string) string {
	if configs == nil {
		return fallback
	}
	if v, ok := configs[key]; ok && v != "" {
		return v
	}
	return fallback
}

// configInt64 parses a numeric broker config value into int64. Returns the
// fallback on any parse error so a bad value never aborts a scan.
func configInt64(configs map[string]string, key string, fallback int64) int64 {
	if configs == nil {
		return fallback
	}
	v, ok := configs[key]
	if !ok {
		return fallback
	}
	var n int64
	if _, err := fmt.Sscanf(strings.TrimSpace(v), "%d", &n); err != nil {
		return fallback
	}
	return n
}
