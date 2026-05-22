package owners

import (
	"context"
	"strings"

	"github.com/conduktor/kafka-attic/internal/types"
)

// topicConfigSource reads a configurable broker topic-config key (default
// "owner") from the collector's cached DescribeConfigs snapshot.
//
// No Kafka calls are issued here: the collector already pulled the key into
// listTopicsResult.configs (see internal/collector/topics.go) when the user
// declared owners.topic_config in kattic.yaml.
type topicConfigSource struct {
	key string
}

func newTopicConfigSource(key string) Source {
	k := strings.TrimSpace(key)
	if k == "" {
		// Appendix B's example uses "owner"; that's the only sensible fallback.
		k = "owner"
	}
	return &topicConfigSource{key: k}
}

func (s *topicConfigSource) Name() string { return SourceTopicConfig }

func (s *topicConfigSource) Lookup(_ context.Context, _ string, topicConfigs map[string]string) (*types.OwnerInfo, error) {
	if topicConfigs == nil {
		return nil, nil
	}
	v, ok := topicConfigs[s.key]
	if !ok {
		return nil, nil
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, nil
	}
	return &types.OwnerInfo{
		Value:  v,
		Source: SourceTopicConfig,
	}, nil
}
