package renderer

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/conduktor/kafka-attic/internal/types"
)

// TestRenderJSONRoundtrip ensures the rendered JSON parses back into a
// Snapshot that matches the input field-for-field on all fields the
// renderer must preserve. Telemetry is generated, so we only assert it
// was populated.
func TestRenderJSONRoundtrip(t *testing.T) {
	in := fixtureSnapshot()

	var buf bytes.Buffer
	if err := RenderJSON(&buf, in, JSONOptions{AnonymousRunUUID: "00000000-0000-4000-8000-000000000001"}); err != nil {
		t.Fatalf("render: %v", err)
	}

	// Pretty-printed: 2-space indent must be present.
	if !bytes.Contains(buf.Bytes(), []byte("\n  \"schema_version\"")) {
		t.Errorf("output is not 2-space indented:\n%s", buf.String())
	}

	var out types.Snapshot
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.SchemaVersion != in.SchemaVersion {
		t.Errorf("schema_version: got %q want %q", out.SchemaVersion, in.SchemaVersion)
	}
	if out.AtticSpecVersion != in.AtticSpecVersion {
		t.Errorf("attic_spec_version: got %q want %q", out.AtticSpecVersion, in.AtticSpecVersion)
	}
	if len(out.Topics) != len(in.Topics) {
		t.Fatalf("topics: got %d want %d", len(out.Topics), len(in.Topics))
	}
	if out.Topics[0].Name != "legacy-events" {
		t.Errorf("first topic: got %q want %q", out.Topics[0].Name, "legacy-events")
	}
	if got := out.Telemetry.AnonymousRunUUID; got == "" {
		t.Errorf("telemetry.anonymous_run_uuid not set")
	}
	if out.Telemetry.SharedSummaryURL != nil {
		t.Errorf("telemetry.shared_summary_url should be null, got %v", *out.Telemetry.SharedSummaryURL)
	}
}

// TestRenderJSONShared sets the shared_summary_url field via opts.
func TestRenderJSONShared(t *testing.T) {
	in := fixtureSnapshot()
	url := "https://attic.conduktor.io/r/abc"
	var buf bytes.Buffer
	if err := RenderJSON(&buf, in, JSONOptions{
		AnonymousRunUUID: "00000000-0000-4000-8000-000000000002",
		SharedSummaryURL: &url,
	}); err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(buf.String(), `"shared_summary_url": "https://attic.conduktor.io/r/abc"`) {
		t.Errorf("shared_summary_url missing in output:\n%s", buf.String())
	}
}

// TestRenderJSONRedactionInJSON ensures redact rewrites topic names but never
// the HTML path (renderer.RenderHTML, when added, must not call applyRedaction).
func TestRenderJSONRedactionInJSON(t *testing.T) {
	in := fixtureSnapshot()
	var buf bytes.Buffer
	if err := RenderJSON(&buf, in, JSONOptions{Redact: true, AnonymousRunUUID: "00000000-0000-4000-8000-000000000003"}); err != nil {
		t.Fatalf("render: %v", err)
	}
	if bytes.Contains(buf.Bytes(), []byte("legacy-events")) {
		t.Errorf("redaction failed: 'legacy-events' present in JSON output")
	}
	// The caller's snapshot must remain unmutated.
	if in.Topics[0].Name != "legacy-events" {
		t.Errorf("caller snapshot mutated: topic[0].Name = %q", in.Topics[0].Name)
	}
}

// TestRenderJSONUUIDGenerated ensures a UUID is generated when none supplied.
func TestRenderJSONUUIDGenerated(t *testing.T) {
	in := fixtureSnapshot()
	var buf bytes.Buffer
	if err := RenderJSON(&buf, in, JSONOptions{}); err != nil {
		t.Fatalf("render: %v", err)
	}
	var out types.Snapshot
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.Telemetry.AnonymousRunUUID) != 36 { // 8-4-4-4-12 dashed UUID
		t.Errorf("generated uuid wrong length: %q", out.Telemetry.AnonymousRunUUID)
	}
}
