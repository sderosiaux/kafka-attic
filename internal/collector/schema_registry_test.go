package collector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/types"
)

// stubSR is a small in-memory SRClient that returns a canned subject list,
// or a canned error. Used for the topic_name / topic_record matching tests.
type stubSR struct {
	subs []string
	err  error
}

func (s *stubSR) ListSubjects(_ context.Context) ([]string, error) {
	return s.subs, s.err
}

// TestCollectSchemaRegistry_TopicNameStrategy_MatchAndOrphan covers the
// canonical SPEC §4.2 Intent case: a topic with both -key and -value
// subjects is "found", a topic with neither is "orphan".
func TestCollectSchemaRegistry_TopicNameStrategy_MatchAndOrphan(t *testing.T) {
	cfg := &config.Config{SchemaRegistry: &config.SchemaRegistryConfig{
		Provider: "confluent", URL: "http://srtest", SubjectStrategy: "topic_name",
	}}
	cli := &stubSR{subs: []string{"orders-key", "orders-value", "unrelated"}}

	res := collectSchemaRegistry(context.Background(), cli, cfg, []string{"orders", "legacy-events"})
	if !res.Configured || !res.Reachable {
		t.Fatalf("want configured+reachable, got %+v", res)
	}
	orders := res.PerTopic["orders"]
	if orders == nil || orders.Evidence != types.EvidenceKnown {
		t.Fatalf("orders missing or wrong evidence: %+v", orders)
	}
	if len(orders.SubjectsFound) != 2 {
		t.Fatalf("orders subjects: want 2, got %v", orders.SubjectsFound)
	}
	legacy := res.PerTopic["legacy-events"]
	if legacy == nil || legacy.Evidence != types.EvidenceKnown {
		t.Fatalf("legacy-events missing or wrong evidence: %+v", legacy)
	}
	if len(legacy.SubjectsFound) != 0 {
		t.Fatalf("legacy-events should be orphan, got %v", legacy.SubjectsFound)
	}
}

// TestCollectSchemaRegistry_TopicRecordStrategy matches any subject prefixed
// `<topic>-`, covering SPEC §4.2 Intent table row for topic_record.
func TestCollectSchemaRegistry_TopicRecordStrategy(t *testing.T) {
	cfg := &config.Config{SchemaRegistry: &config.SchemaRegistryConfig{
		Provider: "confluent", URL: "http://srtest", SubjectStrategy: "topic_record",
	}}
	cli := &stubSR{subs: []string{
		"orders-com.acme.OrderCreated",
		"orders-com.acme.OrderUpdated",
		"shipments-com.acme.Shipped",
	}}

	res := collectSchemaRegistry(context.Background(), cli, cfg, []string{"orders", "shipments", "audit"})
	if len(res.PerTopic["orders"].SubjectsFound) != 2 {
		t.Fatalf("orders subjects: want 2, got %v", res.PerTopic["orders"].SubjectsFound)
	}
	if len(res.PerTopic["shipments"].SubjectsFound) != 1 {
		t.Fatalf("shipments subjects: want 1, got %v", res.PerTopic["shipments"].SubjectsFound)
	}
	if len(res.PerTopic["audit"].SubjectsFound) != 0 {
		t.Fatalf("audit should be orphan, got %v", res.PerTopic["audit"].SubjectsFound)
	}
}

// TestCollectSchemaRegistry_RecordNameSkipped ensures the record_name strategy
// is skipped per SPEC §4.2 (cannot be resolved from SR alone). Every topic
// gets Evidence=UNKNOWN and SkippedStrategy=true on the result.
func TestCollectSchemaRegistry_RecordNameSkipped(t *testing.T) {
	cfg := &config.Config{SchemaRegistry: &config.SchemaRegistryConfig{
		Provider: "confluent", URL: "http://srtest", SubjectStrategy: "record_name",
	}}
	// Even with a reachable client, record_name forces a skip.
	cli := &stubSR{subs: []string{"orders-key"}}

	res := collectSchemaRegistry(context.Background(), cli, cfg, []string{"orders"})
	if !res.SkippedStrategy {
		t.Fatal("expected SkippedStrategy=true for record_name")
	}
	if res.PerTopic["orders"].Evidence != types.EvidenceUnknown {
		t.Fatalf("want UNKNOWN evidence for record_name skip, got %v", res.PerTopic["orders"].Evidence)
	}
}

// TestCollectSchemaRegistry_ListErrorMarksUnknown — SR unreachable with
// on_failure: warn => every topic's Evidence is UNKNOWN, Reachable=false.
func TestCollectSchemaRegistry_ListErrorMarksUnknown(t *testing.T) {
	cfg := &config.Config{SchemaRegistry: &config.SchemaRegistryConfig{
		Provider: "confluent", URL: "http://srtest", SubjectStrategy: "topic_name", OnFailure: "warn",
	}}
	cli := &stubSR{err: errors.New("connect refused")}

	res := collectSchemaRegistry(context.Background(), cli, cfg, []string{"orders"})
	if res.Reachable {
		t.Fatal("expected Reachable=false when ListSubjects errs")
	}
	if res.PerTopic["orders"].Evidence != types.EvidenceUnknown {
		t.Fatal("want UNKNOWN evidence on unreachable SR")
	}
}

// TestCollectSchemaRegistry_NoConfig returns Configured=false so the
// orchestrator can skip the Intent signal entirely.
func TestCollectSchemaRegistry_NoConfig(t *testing.T) {
	res := collectSchemaRegistry(context.Background(), nil, &config.Config{}, []string{"orders"})
	if res.Configured {
		t.Fatal("expected Configured=false when SR not in cfg")
	}
}

// TestHTTPSRClient_HappyPath drives the production HTTP client against a
// httptest server returning the canonical /subjects payload.
func TestHTTPSRClient_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		_ = json.NewEncoder(w).Encode([]string{"a-key", "b-value"})
	}))
	defer srv.Close()

	cli, err := NewSRClient(&config.Config{SchemaRegistry: &config.SchemaRegistryConfig{
		Provider: "confluent", URL: srv.URL, SubjectStrategy: "topic_name",
	}})
	if err != nil {
		t.Fatalf("NewSRClient: %v", err)
	}
	subs, err := cli.ListSubjects(context.Background())
	if err != nil {
		t.Fatalf("ListSubjects: %v", err)
	}
	if len(subs) != 2 || subs[0] != "a-key" || subs[1] != "b-value" {
		t.Fatalf("unexpected subjects: %v", subs)
	}
}

// TestHTTPSRClient_Non2xx returns a non-2xx; client must wrap into an error
// without panicking.
func TestHTTPSRClient_Non2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer srv.Close()

	cli, err := NewSRClient(&config.Config{SchemaRegistry: &config.SchemaRegistryConfig{
		Provider: "confluent", URL: srv.URL, SubjectStrategy: "topic_name",
	}})
	if err != nil {
		t.Fatalf("NewSRClient: %v", err)
	}
	if _, err := cli.ListSubjects(context.Background()); err == nil {
		t.Fatal("expected error on 403")
	}
}

// TestNewSRClient_RejectsNonConfluent enforces the v1 provider whitelist.
func TestNewSRClient_RejectsNonConfluent(t *testing.T) {
	_, err := NewSRClient(&config.Config{SchemaRegistry: &config.SchemaRegistryConfig{
		Provider: "glue", URL: "http://nope",
	}})
	if err == nil {
		t.Fatal("expected error rejecting glue provider in v1")
	}
}
