package scorer

import (
	"testing"

	"github.com/sderosiaux/kafka-attic/internal/config"
	"github.com/sderosiaux/kafka-attic/internal/types"
)

func cfgWithStrategy(s string) *config.Config {
	return &config.Config{
		SchemaRegistry: &config.SchemaRegistryConfig{
			Provider:        "confluent",
			URL:             "https://sr.local",
			SubjectStrategy: s,
		},
	}
}

func TestIntent_TopicName_OrphanIsHigh(t *testing.T) {
	sr := &types.SchemaRegistryInfo{
		SubjectStrategy: "topic_name",
		SubjectsFound:   []string{},
		Evidence:        types.EvidenceKnown,
	}
	score, ev, skipped, ok := scoreIntent(sr, cfgWithStrategy("topic_name"))
	if skipped || !ok {
		t.Fatalf("expected not skipped")
	}
	if score != 100 || ev != types.EvidenceKnown {
		t.Errorf("score=%d ev=%v want 100 KNOWN", score, ev)
	}
}

func TestIntent_TopicName_MatchIsZero(t *testing.T) {
	sr := &types.SchemaRegistryInfo{
		SubjectStrategy: "topic_name",
		SubjectsFound:   []string{"orders-value"},
		Evidence:        types.EvidenceKnown,
	}
	score, _, skipped, _ := scoreIntent(sr, cfgWithStrategy("topic_name"))
	if skipped {
		t.Fatal("expected not skipped")
	}
	if score != 0 {
		t.Errorf("score=%d want 0", score)
	}
}

func TestIntent_TopicRecord_OrphanIsHigh(t *testing.T) {
	sr := &types.SchemaRegistryInfo{
		SubjectStrategy: "topic_record",
		SubjectsFound:   []string{},
		Evidence:        types.EvidenceKnown,
	}
	score, _, _, _ := scoreIntent(sr, cfgWithStrategy("topic_record"))
	if score != 100 {
		t.Errorf("score=%d want 100", score)
	}
}

func TestIntent_TopicRecord_MatchIsZero(t *testing.T) {
	sr := &types.SchemaRegistryInfo{
		SubjectStrategy: "topic_record",
		SubjectsFound:   []string{"orders-com.acme.Order"},
		Evidence:        types.EvidenceKnown,
	}
	score, _, _, _ := scoreIntent(sr, cfgWithStrategy("topic_record"))
	if score != 0 {
		t.Errorf("score=%d want 0", score)
	}
}

func TestIntent_RecordName_Skipped(t *testing.T) {
	sr := &types.SchemaRegistryInfo{
		SubjectStrategy: "record_name",
		Evidence:        types.EvidenceUnknown,
	}
	_, _, skipped, ok := scoreIntent(sr, cfgWithStrategy("record_name"))
	if !skipped || ok {
		t.Errorf("expected skipped=true ok=false; got skipped=%v ok=%v", skipped, ok)
	}
}

func TestIntent_NotConfigured_Skipped(t *testing.T) {
	_, _, skipped, ok := scoreIntent(nil, &config.Config{})
	if !skipped || ok {
		t.Errorf("expected skipped on no SR; got skipped=%v ok=%v", skipped, ok)
	}
}

func TestIntent_Unreachable_Skipped(t *testing.T) {
	sr := &types.SchemaRegistryInfo{
		SubjectStrategy: "topic_name",
		Evidence:        types.EvidenceUnknown,
	}
	_, _, skipped, ok := scoreIntent(sr, cfgWithStrategy("topic_name"))
	if !skipped || ok {
		t.Errorf("expected skipped on unreachable; got skipped=%v ok=%v", skipped, ok)
	}
}
