// Package owners resolves the owner of a Kafka topic from one or more
// configured sources (file, topic config, Backstage, generic JSON HTTP).
//
// The package is read-only: it never produces to Kafka or mutates any
// upstream system. Network sources are HTTP GET only.
//
// SPEC reference: §5.4 + Appendix B.
package owners

import (
	"context"
	"fmt"
	"strings"

	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/types"
)

// Known source names (used in precedence arrays and OwnerInfo.Source).
const (
	SourceFile        = "file"
	SourceTopicConfig = "topic_config"
	SourceBackstage   = "backstage"
	SourceJSON        = "json"
)

// DefaultConcurrency is the per-resolver fan-out across topics when no
// explicit limit is provided.
const DefaultConcurrency = 8

// Source is one configured owner-mapping backend.
//
// Lookup returns:
//   - owner != nil → resolved (caller stops the precedence walk).
//   - owner == nil, err == nil → no mapping for this topic from this source
//     (caller falls through to the next source).
//   - err != nil → soft failure for this topic (caller falls through).
//
// Sources must be safe for concurrent use across topics.
type Source interface {
	// Name returns the source identifier (matches Config.Owners.Precedence).
	Name() string
	// Lookup resolves the owner for one topic, using the supplied topic-config
	// snapshot (key → value) when available.
	Lookup(ctx context.Context, topic string, topicConfigs map[string]string) (*types.OwnerInfo, error)
}

// Resolver is the high-level façade scorers use to attach owners to topics.
type Resolver interface {
	// Resolve returns the owner for one topic, walking the configured
	// precedence and returning the first non-nil result.
	Resolve(ctx context.Context, topic string, topicConfigs map[string]string) *types.OwnerInfo

	// ResolveAll resolves a batch of topics concurrently. The topicConfigs
	// map is indexed by topic name → key → value, exactly as the collector
	// caches it. Topics absent from the map are looked up with a nil config
	// snapshot (topic_config source returns no mapping).
	ResolveAll(ctx context.Context, topics []string, topicConfigs map[string]map[string]string) map[string]*types.OwnerInfo

	// Warnings returns non-fatal messages collected at construction
	// time (e.g. invalid regex entries in the file source). These are
	// surfaced via Snapshot.Scan.MissingSignalsGlobal.
	Warnings() []string
}

// NewFromConfig builds a Resolver from the parsed kattic.yaml config.
// When cfg.Owners is nil or no precedence entries resolve to a configured
// source, a NoopResolver is returned so callers can always invoke Resolve.
func NewFromConfig(cfg *config.Config) (Resolver, error) {
	if cfg == nil || cfg.Owners == nil {
		return &noopResolver{}, nil
	}

	var (
		sources  []Source
		warnings []string
	)

	// Build every configured source, regardless of whether it appears in
	// precedence — we filter by precedence when wiring the resolver. This
	// makes the warning list deterministic.
	if cfg.Owners.File != nil && strings.TrimSpace(cfg.Owners.File.Path) != "" {
		src, ws, err := newFileSource(cfg.Owners.File.Path)
		if err != nil {
			return nil, fmt.Errorf("owners.file: %w", err)
		}
		warnings = append(warnings, ws...)
		sources = append(sources, src)
	}
	if cfg.Owners.TopicConfig != nil && strings.TrimSpace(cfg.Owners.TopicConfig.Key) != "" {
		sources = append(sources, newTopicConfigSource(cfg.Owners.TopicConfig.Key))
	} else if cfg.Owners.TopicConfig != nil {
		// Empty key means the user explicitly enabled topic_config but never
		// set a key; we fall back to "owner" per Appendix B's example.
		sources = append(sources, newTopicConfigSource("owner"))
	}
	if cfg.Owners.Backstage != nil && strings.TrimSpace(cfg.Owners.Backstage.URL) != "" {
		src, err := newBackstageSource(cfg.Owners.Backstage)
		if err != nil {
			return nil, fmt.Errorf("owners.backstage: %w", err)
		}
		sources = append(sources, src)
	}
	if cfg.Owners.JSON != nil && strings.TrimSpace(cfg.Owners.JSON.URL) != "" {
		src, err := newJSONSource(cfg.Owners.JSON)
		if err != nil {
			return nil, fmt.Errorf("owners.json: %w", err)
		}
		sources = append(sources, src)
	}

	precedence := cfg.Owners.Precedence
	if len(precedence) == 0 {
		// SPEC §5.4: precedence is an ordered list. With no list configured
		// we use the source-declaration order which mirrors Appendix B's
		// example: file → topic_config → backstage → json.
		precedence = []string{SourceFile, SourceTopicConfig, SourceBackstage, SourceJSON}
	}

	byName := make(map[string]Source, len(sources))
	for _, s := range sources {
		byName[s.Name()] = s
	}

	ordered := make([]Source, 0, len(precedence))
	for _, name := range precedence {
		if s, ok := byName[name]; ok {
			ordered = append(ordered, s)
		}
		// Unknown source names are silently ignored (the YAML schema is
		// open-ended); precedence to an unconfigured source is a no-op.
	}

	if len(ordered) == 0 {
		return &noopResolver{warnings: warnings}, nil
	}

	return &resolver{
		sources:     ordered,
		concurrency: DefaultConcurrency,
		warnings:    warnings,
	}, nil
}

// noopResolver returns nil for every topic and is used when owners are not
// configured. It still exposes warnings for diagnostics.
type noopResolver struct {
	warnings []string
}

func (n *noopResolver) Resolve(context.Context, string, map[string]string) *types.OwnerInfo {
	return nil
}

func (n *noopResolver) ResolveAll(_ context.Context, topics []string, _ map[string]map[string]string) map[string]*types.OwnerInfo {
	out := make(map[string]*types.OwnerInfo, len(topics))
	for _, t := range topics {
		out[t] = nil
	}
	return out
}

func (n *noopResolver) Warnings() []string { return n.warnings }
