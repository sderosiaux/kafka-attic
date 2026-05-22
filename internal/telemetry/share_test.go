package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

func i64(v int64) *int64 { return &v }

func sampleSnapshot() *types.Snapshot {
	return &types.Snapshot{
		SchemaVersion:     "1.0.0",
		AtticSpecVersion:  "1.0.0",
		KafkaAtticVersion: "1.0.0",
		Scan: types.ScanInfo{
			TopicCountScanned: 4821,
		},
		Topics: []types.Topic{
			{
				Name:    "legacy-events",
				Storage: types.StorageInfo{Bytes: i64(12_300_000_000)},
				Attic:   types.AtticScore{Verdict: types.VerdictCandidate},
			},
			{
				Name:    "orders-v1",
				Storage: types.StorageInfo{Bytes: i64(2_100_000_000)},
				Attic:   types.AtticScore{Verdict: types.VerdictLikelyUnused},
			},
			{
				Name:    "audit-trail",
				Storage: types.StorageInfo{Bytes: i64(890_000_000)},
				Attic:   types.AtticScore{Verdict: types.VerdictActive},
			},
			{
				Name:              "excluded",
				ExcludedByPattern: true,
				Attic:             types.AtticScore{Verdict: types.VerdictLikelyUnused},
			},
			{
				Name:    "empty-topic",
				Storage: types.StorageInfo{Bytes: i64(0)},
				Attic:   types.AtticScore{Verdict: types.VerdictCandidate},
			},
		},
	}
}

func TestBuildSharePayload_RedactsAndBuckets(t *testing.T) {
	snap := sampleSnapshot()
	p, err := BuildSharePayload("1.0.0", "test-uuid", snap)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// Verdict counts: LIKELY_UNUSED=1 (orders-v1; excluded one filtered),
	// CANDIDATE=2, INSPECT=0, ACTIVE=1.
	want := map[string]int{
		"LIKELY_UNUSED": 1,
		"CANDIDATE":     2,
		"INSPECT":       0,
		"ACTIVE":        1,
	}
	for k, v := range want {
		if p.VerdictCounts[k] != v {
			t.Errorf("verdict_counts[%s]: got %d want %d", k, p.VerdictCounts[k], v)
		}
	}

	// Reclaimable = 12.3B + 2.1B + 0 = ~14.4B bytes → bucket starts with 1e10.
	if !strings.HasPrefix(p.ReclaimableBucket, "10000000000-") {
		t.Errorf("reclaimable bucket should be 10^10 range, got %q", p.ReclaimableBucket)
	}

	if p.TopicCountBucket != Bucket1kTo10k {
		t.Errorf("topic_count_bucket: got %q want %q", p.TopicCountBucket, Bucket1kTo10k)
	}
	if p.SchemaVersion != ShareSchemaVersion {
		t.Errorf("schema_version: got %q", p.SchemaVersion)
	}
}

func TestBuildSharePayload_NilSnapshotReturnsError(t *testing.T) {
	if _, err := BuildSharePayload("1.0", "uuid", nil); err == nil {
		t.Fatal("expected error on nil snapshot")
	}
}

func TestBucketBytesPow10(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{-1, "0"},
		{0, "0"},
		{1, "1-9"},
		{9, "1-9"},
		{10, "10-99"},
		{99, "10-99"},
		{100, "100-999"},
		{1234, "1000-9999"},
		{14_400_000_000, "10000000000-99999999999"},
	}
	for _, c := range cases {
		if got := BucketBytesPow10(c.n); got != c.want {
			t.Errorf("BucketBytesPow10(%d) = %q want %q", c.n, got, c.want)
		}
	}
}

func TestAssertNoPIIShare_RejectsUnknownVerdict(t *testing.T) {
	p, err := BuildSharePayload("1.0", "uuid", sampleSnapshot())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	p.VerdictCounts["NOT_A_VERDICT"] = 1
	if err := AssertNoPIIShare(p); err == nil {
		t.Fatal("expected rejection of unknown verdict")
	}
}

func TestSharer_Send_PayloadSchemaAndResponse(t *testing.T) {
	var capturedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		capturedBody = b
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"url":"https://attic.conduktor.io/r/abc123","id":"abc123"}`))
	}))
	defer srv.Close()

	s := NewSharer(srv.URL)
	payload, err := BuildSharePayload("1.0.0", "test-uuid", sampleSnapshot())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := s.Send(ctx, payload)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if !strings.HasPrefix(resp.URL, "https://attic.conduktor.io/r/") {
		t.Fatalf("response URL shape: got %q", resp.URL)
	}

	// Schema check on the actual wire bytes.
	var wire map[string]any
	if err := json.Unmarshal(capturedBody, &wire); err != nil {
		t.Fatalf("unmarshal wire: %v", err)
	}
	for k := range wire {
		found := slices.Contains(AllowedShareKeys, k)
		if !found {
			t.Fatalf("share wire carries disallowed key %q", k)
		}
	}
	// Topic names must not appear anywhere in the body.
	for _, banned := range []string{"legacy-events", "orders-v1", "audit-trail", "empty-topic", "excluded"} {
		if strings.Contains(string(capturedBody), banned) {
			t.Fatalf("share wire contains topic name %q: %s", banned, capturedBody)
		}
	}
}

func TestSharer_Send_RejectsEmptyURLResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"url":""}`))
	}))
	defer srv.Close()

	s := NewSharer(srv.URL)
	payload, _ := BuildSharePayload("1.0", "uuid", sampleSnapshot())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.Send(ctx, payload); err == nil {
		t.Fatal("expected error on empty URL")
	}
}

func TestSharer_Send_HTTPErrorPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	s := NewSharer(srv.URL)
	payload, _ := BuildSharePayload("1.0", "uuid", sampleSnapshot())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.Send(ctx, payload); err == nil {
		t.Fatal("expected error on 400")
	}
}
