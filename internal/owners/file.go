package owners

import (
	"context"
	"fmt"
	"os"
	"regexp"

	"github.com/conduktor/kafka-attic/internal/types"
	"gopkg.in/yaml.v3"
)

// fileEntry mirrors one row in owners.yaml.
//
// Appendix B example:
//
//	{ pattern: '^orders-.*', owner: 'team-orders@acme.com' }
type fileEntry struct {
	Pattern string `yaml:"pattern"`
	Owner   string `yaml:"owner"`
}

// fileSource implements first-match regex lookup over a static YAML list.
type fileSource struct {
	entries []compiledEntry
}

type compiledEntry struct {
	pattern *regexp.Regexp
	owner   string
	raw     string
}

func newFileSource(path string) (Source, []string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", path, err)
	}

	// We accept two shapes for forward-compat with users who wrap the list
	// in a top-level `entries:` key. The bare list is the canonical form.
	var (
		bare    []fileEntry
		wrapped struct {
			Entries []fileEntry `yaml:"entries"`
		}
	)
	if err := yaml.Unmarshal(data, &bare); err != nil || len(bare) == 0 {
		if werr := yaml.Unmarshal(data, &wrapped); werr == nil && len(wrapped.Entries) > 0 {
			bare = wrapped.Entries
		} else if err != nil && len(wrapped.Entries) == 0 {
			return nil, nil, fmt.Errorf("parse %s: %w", path, err)
		}
	}

	var (
		compiled []compiledEntry
		warnings []string
	)
	for _, e := range bare {
		if e.Pattern == "" {
			warnings = append(warnings, fmt.Sprintf("owners.file: skipping entry with empty pattern (owner=%q)", e.Owner))
			continue
		}
		re, rerr := regexp.Compile(e.Pattern)
		if rerr != nil {
			// SPEC §5.4: invalid patterns log a warning and are skipped.
			warnings = append(warnings, fmt.Sprintf("owners.file: invalid pattern %q: %v", e.Pattern, rerr))
			continue
		}
		compiled = append(compiled, compiledEntry{pattern: re, owner: e.Owner, raw: e.Pattern})
	}

	return &fileSource{entries: compiled}, warnings, nil
}

func (s *fileSource) Name() string { return SourceFile }

// Lookup returns the owner of the first compiled entry whose pattern
// matches the topic. Entries are evaluated in file order; first match wins.
func (s *fileSource) Lookup(_ context.Context, topic string, _ map[string]string) (*types.OwnerInfo, error) {
	for _, e := range s.entries {
		if e.pattern.MatchString(topic) {
			if e.owner == "" {
				// Pattern matched but owner is empty — treat as no mapping
				// rather than a blank owner, so the precedence walk
				// continues.
				return nil, nil
			}
			return &types.OwnerInfo{
				Value:  e.owner,
				Source: SourceFile,
			}, nil
		}
	}
	return nil, nil
}
