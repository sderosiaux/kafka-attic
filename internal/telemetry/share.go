package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/conduktor/kafka-attic/internal/types"
)

// ShareTimeout is the deadline on a --share upload. Larger than PingTimeout
// because the payload is bigger and the user is actively waiting for the URL.
const ShareTimeout = 15 * time.Second

// DefaultShareEndpoint is the HTTP endpoint that receives anonymized share
// summaries. The response URL lives under attic.conduktor.io/r/<id> per SPEC §5.7.
const DefaultShareEndpoint = "https://telemetry.conduktor.io/attic/share"

// SharePayload is the EXACT shape uploaded by `kattic audit --share`.
//
// What is sent (per SPEC §5.7):
//   - per-verdict counts
//   - reclaimable bytes bucketed to powers of 10
//   - cluster size bucket
//   - kafka-attic version, OS, anonymous run UUID
//
// What is NEVER sent: topic names, broker addresses, owner data, schema
// subject names, raw byte totals, raw topic counts.
type SharePayload struct {
	Version            string             `json:"version"`
	OS                 string             `json:"os"`
	RunUUID            string             `json:"run_uuid"`
	ClusterSizeBucket  ClusterSizeBucket  `json:"cluster_size_bucket"`
	VerdictCounts      map[string]int     `json:"verdict_counts"`
	ReclaimableBucket  string             `json:"reclaimable_bytes_bucket"`
	TopicCountBucket   ClusterSizeBucket  `json:"topic_count_bucket"`
	SchemaVersion      string             `json:"schema_version"`
}

// ShareSchemaVersion versions the share payload schema independently of the
// snapshot schema; the receiving service uses it to route incoming uploads.
const ShareSchemaVersion = "1.0.0"

// AllowedShareKeys is the closed set of JSON keys a SharePayload may carry.
// Used by share_test.go to verify nothing else slips through.
var AllowedShareKeys = []string{
	"version",
	"os",
	"run_uuid",
	"cluster_size_bucket",
	"verdict_counts",
	"reclaimable_bytes_bucket",
	"topic_count_bucket",
	"schema_version",
}

// ShareResponse is what the endpoint replies with: a stable URL to the
// hosted summary. The struct exists so callers can ignore the raw HTTP body.
type ShareResponse struct {
	URL string `json:"url"`
	ID  string `json:"id,omitempty"`
}

// Sharer uploads anonymized share summaries.
type Sharer struct {
	Endpoint string
	Client   *http.Client
}

// NewSharer returns a Sharer pointed at endpoint with the 15s timeout.
// An empty endpoint falls back to DefaultShareEndpoint.
func NewSharer(endpoint string) *Sharer {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = DefaultShareEndpoint
	}
	return &Sharer{
		Endpoint: endpoint,
		Client:   &http.Client{Timeout: ShareTimeout},
	}
}

// BuildSharePayload derives an anonymized SharePayload from a full snapshot.
// The caller passes the version, run UUID, and snapshot — this function
// performs the redaction (bucketing, count-by-verdict, dropping names).
func BuildSharePayload(version, runUUID string, snap *types.Snapshot) (SharePayload, error) {
	if snap == nil {
		return SharePayload{}, errors.New("snapshot is nil")
	}
	counts := make(map[string]int, 4)
	// Seed the four verdicts so the receiving end always sees them — zero
	// values are meaningful (no Likely-Unused topics is itself a signal).
	for _, v := range []types.Verdict{
		types.VerdictLikelyUnused,
		types.VerdictCandidate,
		types.VerdictInspect,
		types.VerdictActive,
	} {
		counts[string(v)] = 0
	}
	var reclaimable int64
	for _, t := range snap.Topics {
		if t.ExcludedByPattern {
			continue
		}
		v := string(t.Attic.Verdict)
		if v == "" {
			continue
		}
		counts[v]++
		// Reclaimable = bytes in topics verdicted LIKELY_UNUSED or CANDIDATE.
		if (t.Attic.Verdict == types.VerdictLikelyUnused || t.Attic.Verdict == types.VerdictCandidate) &&
			t.Storage.Bytes != nil && *t.Storage.Bytes > 0 {
			reclaimable += *t.Storage.Bytes
		}
	}
	return SharePayload{
		Version:           strings.TrimSpace(version),
		OS:                osName(),
		RunUUID:           runUUID,
		ClusterSizeBucket: BucketFor(snap.Scan.TopicCountScanned),
		VerdictCounts:     counts,
		ReclaimableBucket: BucketBytesPow10(reclaimable),
		TopicCountBucket:  BucketFor(snap.Scan.TopicCountScanned),
		SchemaVersion:     ShareSchemaVersion,
	}, nil
}

// osName is a package-level seam for tests; production returns runtime.GOOS.
var osName = func() string { return runtime.GOOS }

// BucketBytesPow10 returns a power-of-10 bucket string for a byte total.
//
// Buckets: "0", "1-9", "10-99", "100-999", "1000-9999", … up to "1e18+".
//
// We never return the raw number — the bucket is the unit of disclosure.
// Negative inputs return "0" (defensive; reclaimable cannot be negative).
func BucketBytesPow10(n int64) string {
	if n <= 0 {
		return "0"
	}
	// floor(log10(n))
	exp := int(math.Floor(math.Log10(float64(n))))
	if exp < 0 {
		exp = 0
	}
	if exp >= 19 {
		return "1e19+"
	}
	lo := int64(math.Pow10(exp))
	hi := int64(math.Pow10(exp+1)) - 1
	if exp == 0 {
		return "1-9"
	}
	return fmt.Sprintf("%d-%d", lo, hi)
}

// Send uploads the payload and returns the parsed ShareResponse.
func (s *Sharer) Send(ctx context.Context, payload SharePayload) (ShareResponse, error) {
	if s == nil {
		return ShareResponse{}, errors.New("nil sharer")
	}
	if err := AssertNoPIIShare(payload); err != nil {
		return ShareResponse{}, err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return ShareResponse{}, fmt.Errorf("marshal share: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Endpoint, bytes.NewReader(body))
	if err != nil {
		return ShareResponse{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "kafka-attic/"+payload.Version)
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: ShareTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return ShareResponse{}, fmt.Errorf("post share: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return ShareResponse{}, fmt.Errorf("share rejected: status %d", resp.StatusCode)
	}
	var out ShareResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return ShareResponse{}, fmt.Errorf("decode share response: %w", err)
	}
	if strings.TrimSpace(out.URL) == "" {
		return ShareResponse{}, errors.New("share response missing url")
	}
	return out, nil
}

// AssertNoPIIShare validates a SharePayload against the SPEC §5.7 redaction
// rules. Returns an error if any banned content is present.
func AssertNoPIIShare(payload SharePayload) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal for pii check: %w", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("unmarshal for pii check: %w", err)
	}
	allowed := make(map[string]struct{}, len(AllowedShareKeys))
	for _, k := range AllowedShareKeys {
		allowed[k] = struct{}{}
	}
	for k := range raw {
		if _, ok := allowed[k]; !ok {
			return fmt.Errorf("share payload contains disallowed key %q", k)
		}
	}
	// Verdict counts: keys must be from the closed verdict enum.
	for k := range payload.VerdictCounts {
		switch types.Verdict(k) {
		case types.VerdictLikelyUnused, types.VerdictCandidate, types.VerdictInspect, types.VerdictActive:
			// ok
		default:
			return fmt.Errorf("share payload verdict_counts has unknown verdict %q", k)
		}
	}
	return nil
}
