package owners

import (
	"context"
	"sync"

	"github.com/conduktor/kafka-attic/internal/types"
)

// resolver walks an ordered list of sources, first non-nil wins.
type resolver struct {
	sources     []Source
	concurrency int
	warnings    []string
}

func (r *resolver) Warnings() []string { return r.warnings }

// Resolve returns the first non-nil owner across the configured precedence.
// A source returning an error is treated as "no mapping" — owner resolution
// is best-effort and must never fail the audit.
func (r *resolver) Resolve(ctx context.Context, topic string, topicConfigs map[string]string) *types.OwnerInfo {
	for _, s := range r.sources {
		if ctx.Err() != nil {
			return nil
		}
		owner, err := s.Lookup(ctx, topic, topicConfigs)
		if err != nil {
			// Soft-fail: try the next source.
			continue
		}
		if owner != nil {
			return owner
		}
	}
	return nil
}

// ResolveAll fans out across topics with a bounded worker pool.
//
// The concurrency limit applies across topics, not across sources for a
// single topic — each topic still walks its precedence sequentially so
// that a cheap source (file) can short-circuit before a network source.
func (r *resolver) ResolveAll(ctx context.Context, topics []string, topicConfigs map[string]map[string]string) map[string]*types.OwnerInfo {
	out := make(map[string]*types.OwnerInfo, len(topics))
	if len(topics) == 0 {
		return out
	}

	concurrency := r.concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	if concurrency > len(topics) {
		concurrency = len(topics)
	}

	type result struct {
		topic string
		owner *types.OwnerInfo
	}

	jobs := make(chan string, len(topics))
	results := make(chan result, len(topics))

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for topic := range jobs {
				owner := r.Resolve(ctx, topic, topicConfigs[topic])
				results <- result{topic: topic, owner: owner}
			}
		}()
	}

	for _, t := range topics {
		jobs <- t
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		out[res.topic] = res.owner
	}
	return out
}
