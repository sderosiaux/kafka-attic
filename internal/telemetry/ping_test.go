package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBuildPayload_StripsFlagValues(t *testing.T) {
	in := PingInput{
		Version:    "1.0.0",
		Flags:      []string{"--cluster=prod.yaml", "--share", "--output=/tmp/report.html", "--share", ""},
		TopicCount: 1500,
		ExitCode:   0,
	}
	p, err := BuildPayload(in)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, f := range p.Flags {
		if strings.ContainsRune(f, '=') {
			t.Fatalf("flag %q still carries value", f)
		}
	}
	// Deduplication + sort: --cluster, --output, --share (sorted).
	want := []string{"--cluster", "--output", "--share"}
	if len(p.Flags) != len(want) {
		t.Fatalf("flag count: got %v want %v", p.Flags, want)
	}
	for i := range want {
		if p.Flags[i] != want[i] {
			t.Fatalf("flag[%d]: got %q want %q", i, p.Flags[i], want[i])
		}
	}
	if p.ClusterSizeBucket != Bucket1kTo10k {
		t.Fatalf("bucket: got %q want %q", p.ClusterSizeBucket, Bucket1kTo10k)
	}
}

func TestBucketFor(t *testing.T) {
	cases := []struct {
		n    int
		want ClusterSizeBucket
	}{
		{-1, BucketUnknown},
		{0, BucketUnknown},
		{1, Bucket1To100},
		{100, Bucket1To100},
		{101, Bucket100To1k},
		{1000, Bucket100To1k},
		{1001, Bucket1kTo10k},
		{10000, Bucket1kTo10k},
		{10001, Bucket10kPlus},
		{999999, Bucket10kPlus},
	}
	for _, c := range cases {
		if got := BucketFor(c.n); got != c.want {
			t.Errorf("BucketFor(%d) = %q want %q", c.n, got, c.want)
		}
	}
}

func TestNewRunUUID_FormatAndUniqueness(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		u, err := newRunUUID()
		if err != nil {
			t.Fatalf("uuid: %v", err)
		}
		if !re.MatchString(u) {
			t.Fatalf("uuid %q does not match v4 shape", u)
		}
		if _, dup := seen[u]; dup {
			t.Fatalf("duplicate uuid %q", u)
		}
		seen[u] = struct{}{}
	}
}

// TestAssertNoPII_PassesOnCleanPayload is the happy path: a payload whose
// only fields are in the allowlist passes.
func TestAssertNoPII_PassesOnCleanPayload(t *testing.T) {
	p, err := BuildPayload(PingInput{
		Version:    "1.0.0",
		Flags:      []string{"--cluster", "--share"},
		TopicCount: 50,
		ExitCode:   0,
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := AssertNoPII(p); err != nil {
		t.Fatalf("clean payload rejected: %v", err)
	}
}

// TestAssertNoPII_RejectsFlagWithValue is the core redaction assertion: if a
// caller smuggles a value-bearing flag into the payload, AssertNoPII must
// reject it before it reaches the wire.
func TestAssertNoPII_RejectsFlagWithValue(t *testing.T) {
	p, err := BuildPayload(PingInput{Version: "1.0.0", TopicCount: 1})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// Bypass the constructor to simulate a caller mutating the struct
	// after BuildPayload. This is the case AssertNoPII must catch.
	p.Flags = []string{"--cluster=prod.yaml"}
	if err := AssertNoPII(p); err == nil {
		t.Fatal("expected PII assertion to fail on flag with value")
	}
}

// TestAssertNoPII_RejectsPathChars guards against accidental leakage of file
// paths or emails through any string field.
func TestAssertNoPII_RejectsPathChars(t *testing.T) {
	cases := []PingPayload{
		{Version: "/tmp/secret", OS: "linux", Arch: "amd64", RunUUID: "id"},
		{Version: "1.0", OS: "linux", Arch: "amd64", RunUUID: "user@host"},
		{Version: "1.0", OS: `c:\users`, Arch: "amd64", RunUUID: "id"},
	}
	for i, c := range cases {
		if err := AssertNoPII(c); err == nil {
			t.Errorf("case %d: expected rejection, got nil", i)
		}
	}
}

func TestPinger_Send_PostsExpectedShape(t *testing.T) {
	var calls atomic.Int32
	var capturedBody []byte
	var capturedCT string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		capturedCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		capturedBody = b
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p := NewPinger(srv.URL)
	payload, err := BuildPayload(PingInput{
		Version:    "1.2.3",
		Flags:      []string{"--cluster=prod.yaml", "--share"},
		TopicCount: 250,
		ExitCode:   0,
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Send(ctx, payload); err != nil {
		t.Fatalf("send: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", calls.Load())
	}
	if capturedCT != "application/json" {
		t.Fatalf("content-type: got %q", capturedCT)
	}

	// Parse and audit the wire payload — this is the redaction asserter
	// applied to what actually went over the network.
	var wire map[string]any
	if err := json.Unmarshal(capturedBody, &wire); err != nil {
		t.Fatalf("unmarshal wire: %v (body=%s)", err, capturedBody)
	}
	for k := range wire {
		found := false
		for _, allowed := range AllowedPayloadKeys {
			if k == allowed {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("wire payload carries disallowed key %q", k)
		}
	}
	flags, _ := wire["flags"].([]any)
	for _, f := range flags {
		if s, ok := f.(string); ok && strings.ContainsRune(s, '=') {
			t.Fatalf("wire flag carries value: %q", s)
		}
	}
	// Verify the bucket — not the raw count.
	if got := wire["cluster_size_bucket"]; got != string(Bucket100To1k) {
		t.Fatalf("cluster_size_bucket: got %v want %q", got, Bucket100To1k)
	}
	if _, ok := wire["topic_count"]; ok {
		t.Fatal("wire payload must not contain raw topic_count")
	}
}

func TestPinger_Send_Times_OutFast(t *testing.T) {
	// Handler hangs until the test releases it. Cleanup order is LIFO: we
	// register srv.Close FIRST, then close(release) SECOND — so on teardown
	// release closes first, the handler returns, then srv.Close finishes.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	p := NewPinger(srv.URL)
	p.Client = &http.Client{Timeout: 50 * time.Millisecond}

	payload, _ := BuildPayload(PingInput{Version: "1.0", TopicCount: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := p.Send(ctx, payload)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Send took too long: %s", d)
	}
}
