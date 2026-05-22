package owners

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/types"
)

// stubSource lets us script per-topic responses.
type stubSource struct {
	name    string
	answers map[string]*types.OwnerInfo
	errs    map[string]error
	calls   atomic.Int64
}

func (s *stubSource) Name() string { return s.name }
func (s *stubSource) Lookup(_ context.Context, topic string, _ map[string]string) (*types.OwnerInfo, error) {
	s.calls.Add(1)
	if e, ok := s.errs[topic]; ok {
		return nil, e
	}
	if v, ok := s.answers[topic]; ok {
		return v, nil
	}
	return nil, nil
}

func newStub(name string, answers map[string]string) *stubSource {
	a := make(map[string]*types.OwnerInfo, len(answers))
	for k, v := range answers {
		a[k] = &types.OwnerInfo{Value: v, Source: name}
	}
	return &stubSource{name: name, answers: a}
}

func TestResolver_PrecedenceFirstNonNilWins(t *testing.T) {
	first := newStub("first", map[string]string{
		"orders-1": "first-owner",
	})
	second := newStub("second", map[string]string{
		"orders-1": "second-owner",
		"orders-2": "second-only",
	})
	third := newStub("third", map[string]string{
		"orders-3": "third-only",
	})

	r := &resolver{sources: []Source{first, second, third}, concurrency: 1}

	// orders-1 resolved by first; second and third should still not be called for it
	// (we only test that the value is "first-owner"; call counts include other topics).
	owner := r.Resolve(context.Background(), "orders-1", nil)
	if owner == nil || owner.Value != "first-owner" {
		t.Fatalf("orders-1: expected first-owner, got %+v", owner)
	}

	owner = r.Resolve(context.Background(), "orders-2", nil)
	if owner == nil || owner.Value != "second-only" {
		t.Fatalf("orders-2: expected second-only, got %+v", owner)
	}

	owner = r.Resolve(context.Background(), "orders-3", nil)
	if owner == nil || owner.Value != "third-only" {
		t.Fatalf("orders-3: expected third-only, got %+v", owner)
	}

	owner = r.Resolve(context.Background(), "unknown", nil)
	if owner != nil {
		t.Fatalf("unknown: expected nil, got %+v", owner)
	}
}

func TestResolver_ErrorIsSoftFail(t *testing.T) {
	first := &stubSource{
		name: "first",
		errs: map[string]error{"orders-1": errors.New("network down")},
	}
	second := newStub("second", map[string]string{"orders-1": "fallback"})
	r := &resolver{sources: []Source{first, second}, concurrency: 1}

	owner := r.Resolve(context.Background(), "orders-1", nil)
	if owner == nil || owner.Value != "fallback" {
		t.Fatalf("expected fallback after first errors, got %+v", owner)
	}
}

func TestResolver_ResolveAllConcurrent(t *testing.T) {
	const n = 50
	answers := make(map[string]string, n)
	topics := make([]string, n)
	for i := 0; i < n; i++ {
		topic := fmt.Sprintf("t-%d", i)
		topics[i] = topic
		answers[topic] = fmt.Sprintf("owner-%d", i)
	}
	src := newStub("only", answers)
	r := &resolver{sources: []Source{src}, concurrency: 8}

	out := r.ResolveAll(context.Background(), topics, nil)
	if len(out) != n {
		t.Fatalf("expected %d results, got %d", n, len(out))
	}
	for i, topic := range topics {
		got := out[topic]
		if got == nil || got.Value != fmt.Sprintf("owner-%d", i) {
			t.Errorf("%s: got %+v", topic, got)
		}
	}
	if got := src.calls.Load(); got != int64(n) {
		t.Errorf("expected %d source calls, got %d", n, got)
	}
}

func TestResolver_ResolveAllPassesTopicConfigs(t *testing.T) {
	src := &stubSource{
		name: "tc",
		answers: map[string]*types.OwnerInfo{},
	}
	// Custom source that reads from the per-topic configs map directly.
	configReader := &configReaderSource{key: "owner"}
	r := &resolver{sources: []Source{src, configReader}, concurrency: 4}

	topics := []string{"a", "b"}
	configs := map[string]map[string]string{
		"a": {"owner": "team-a"},
		"b": {"owner": "team-b"},
	}
	out := r.ResolveAll(context.Background(), topics, configs)
	if out["a"] == nil || out["a"].Value != "team-a" {
		t.Errorf("a: got %+v", out["a"])
	}
	if out["b"] == nil || out["b"].Value != "team-b" {
		t.Errorf("b: got %+v", out["b"])
	}
}

// configReaderSource is a tiny in-test Source used to verify per-topic
// configs are routed correctly by ResolveAll.
type configReaderSource struct {
	mu  sync.Mutex
	key string
}

func (c *configReaderSource) Name() string { return "config-reader" }
func (c *configReaderSource) Lookup(_ context.Context, _ string, configs map[string]string) (*types.OwnerInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if configs == nil {
		return nil, nil
	}
	if v, ok := configs[c.key]; ok {
		return &types.OwnerInfo{Value: v, Source: c.Name()}, nil
	}
	return nil, nil
}

func TestNewFromConfig_NilOwnersIsNoop(t *testing.T) {
	r, err := NewFromConfig(&config.Config{})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	if r.Resolve(context.Background(), "anything", nil) != nil {
		t.Error("noop resolver returned non-nil")
	}
	out := r.ResolveAll(context.Background(), []string{"x", "y"}, nil)
	if len(out) != 2 || out["x"] != nil || out["y"] != nil {
		t.Errorf("noop ResolveAll: %+v", out)
	}
}

func TestNewFromConfig_DefaultPrecedenceWhenEmpty(t *testing.T) {
	cfg := &config.Config{
		Owners: &config.OwnersConfig{
			TopicConfig: &config.OwnersTopicConfig{Key: "owner"},
		},
	}
	r, err := NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	owner := r.Resolve(context.Background(), "t", map[string]string{"owner": "team-z"})
	if owner == nil || owner.Value != "team-z" {
		t.Fatalf("expected team-z, got %+v", owner)
	}
}

func TestNewFromConfig_FilePropagatesWarnings(t *testing.T) {
	path := writeOwnersFile(t, `
- pattern: '['
  owner: bad
- pattern: '^x'
  owner: ok
`)
	cfg := &config.Config{
		Owners: &config.OwnersConfig{
			Precedence: []string{SourceFile},
			File:       &config.OwnersFileConfig{Path: path},
		},
	}
	r, err := NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	warns := r.Warnings()
	if len(warns) != 1 {
		t.Fatalf("expected 1 warning, got %v", warns)
	}
}
