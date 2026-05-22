package owners

import (
	"context"
	"testing"
)

func TestTopicConfigSource_ReadsConfiguredKey(t *testing.T) {
	src := newTopicConfigSource("owner")
	owner, err := src.Lookup(context.Background(), "orders-1", map[string]string{
		"owner":           "team-orders@acme.com",
		"retention.ms":    "604800000",
		"cleanup.policy":  "delete",
	})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner == nil {
		t.Fatal("expected owner from topic config")
	}
	if owner.Value != "team-orders@acme.com" {
		t.Errorf("value mismatch: got %q", owner.Value)
	}
	if owner.Source != SourceTopicConfig {
		t.Errorf("source mismatch: got %q", owner.Source)
	}
}

func TestTopicConfigSource_AbsentKey(t *testing.T) {
	src := newTopicConfigSource("owner")
	owner, err := src.Lookup(context.Background(), "orders-1", map[string]string{
		"retention.ms": "604800000",
	})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner != nil {
		t.Fatalf("expected nil for missing key, got %+v", owner)
	}
}

func TestTopicConfigSource_EmptyValueTreatedAsAbsent(t *testing.T) {
	src := newTopicConfigSource("owner")
	owner, err := src.Lookup(context.Background(), "orders-1", map[string]string{
		"owner": "   ",
	})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner != nil {
		t.Fatalf("expected nil for whitespace-only value, got %+v", owner)
	}
}

func TestTopicConfigSource_NilConfigs(t *testing.T) {
	src := newTopicConfigSource("owner")
	owner, err := src.Lookup(context.Background(), "orders-1", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner != nil {
		t.Fatalf("expected nil with no configs, got %+v", owner)
	}
}

func TestTopicConfigSource_EmptyKeyFallsBackToOwner(t *testing.T) {
	src := newTopicConfigSource("")
	owner, _ := src.Lookup(context.Background(), "orders-1", map[string]string{
		"owner": "team-orders@acme.com",
	})
	if owner == nil || owner.Value != "team-orders@acme.com" {
		t.Fatalf("expected default key 'owner' to be used, got %+v", owner)
	}
}
