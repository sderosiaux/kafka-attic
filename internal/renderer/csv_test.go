package renderer

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderCSVGolden(t *testing.T) {
	snap := fixtureSnapshot()
	var buf bytes.Buffer
	if err := RenderCSV(&buf, snap, CSVOptions{}); err != nil {
		t.Fatalf("render: %v", err)
	}
	goldenPath := filepath.Join("testdata", "csv.golden")
	if *updateGolden {
		err := os.WriteFile(goldenPath, buf.Bytes(), 0o644)
		if err != nil {
			t.Fatalf("write golden: %v", err)
		}
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (run with -update first?): %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("csv mismatch.\n--- got ---\n%s\n--- want ---\n%s", buf.String(), string(want))
	}

	// Sanity: CSV must parse and have the right number of rows.
	r := csv.NewReader(bytes.NewReader(buf.Bytes()))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("re-parse csv: %v", err)
	}
	if len(records) != 1+len(snap.Topics) {
		t.Errorf("records: got %d, want %d", len(records), 1+len(snap.Topics))
	}
	if records[0][0] != "name" {
		t.Errorf("first column header: got %q want %q", records[0][0], "name")
	}
}

func TestRenderCSVRedact(t *testing.T) {
	snap := fixtureSnapshot()
	var buf bytes.Buffer
	if err := RenderCSV(&buf, snap, CSVOptions{Redact: true}); err != nil {
		t.Fatalf("render: %v", err)
	}
	if strings.Contains(buf.String(), "legacy-events") {
		t.Errorf("redaction failed: 'legacy-events' present in CSV output")
	}
	// First name in output should be the SHA-256 of "legacy-events".
	r := csv.NewReader(bytes.NewReader(buf.Bytes()))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if rows[1][0] != sha256Hex("legacy-events") {
		t.Errorf("first row name: got %q want %q", rows[1][0], sha256Hex("legacy-events"))
	}
}

func TestCSVColumnOrderStable(t *testing.T) {
	// Pin the column order so future schema additions only append.
	want := []string{
		"name", "partitions", "replication_factor", "cleanup_policy",
		"retention_ms", "remote_storage_enabled", "message_timestamp_type",
		"last_produce_ts", "earliest_offset_sum", "latest_offset_sum",
		"storage_bytes", "storage_source", "storage_evidence",
		"score_activity", "score_tenancy", "score_tonnage", "score_intent", "score_consumption",
		"raw_score", "verdict", "verdict_capped_by", "flags",
		"owner", "owner_source", "signals_missing",
	}
	if len(csvColumns) != len(want) {
		t.Fatalf("column count: got %d want %d", len(csvColumns), len(want))
	}
	for i, c := range want {
		if csvColumns[i] != c {
			t.Errorf("column[%d]: got %q want %q", i, csvColumns[i], c)
		}
	}
}
