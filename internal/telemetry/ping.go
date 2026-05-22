package telemetry

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"time"
)

// DefaultEndpoint is the production telemetry endpoint per SPEC §11.
const DefaultEndpoint = "https://telemetry.conduktor.io/attic"

// PingTimeout caps fire-and-forget pings so a hung endpoint never delays a run.
const PingTimeout = 5 * time.Second

// ClusterSizeBucket is the coarse topic-count bucket sent with each ping.
// Bucketing protects against fingerprinting individual clusters by topic count.
type ClusterSizeBucket string

// ClusterSizeBucket enum values.
const (
	BucketUnknown ClusterSizeBucket = "unknown"
	Bucket1To100  ClusterSizeBucket = "1-100"
	Bucket100To1k ClusterSizeBucket = "100-1k"
	Bucket1kTo10k ClusterSizeBucket = "1k-10k"
	Bucket10kPlus ClusterSizeBucket = "10k+"
)

// BucketFor returns the ClusterSizeBucket for a given topic count.
//
// Boundaries are inclusive-lower exclusive-upper to match SPEC §5.7 labels:
//
//	[1,100], (100,1000], (1000,10000], (10000,+inf)
//
// Zero or negative inputs return BucketUnknown — refusing to invent a bucket.
func BucketFor(topicCount int) ClusterSizeBucket {
	switch {
	case topicCount <= 0:
		return BucketUnknown
	case topicCount <= 100:
		return Bucket1To100
	case topicCount <= 1000:
		return Bucket100To1k
	case topicCount <= 10000:
		return Bucket1kTo10k
	default:
		return Bucket10kPlus
	}
}

// PingPayload is the EXACT shape sent on the wire. The struct definition is
// the source of truth for what telemetry can carry. Any field not listed here
// cannot be sent — and any caller-provided string is normalized before send.
//
// PII redaction strategy:
//
//   - Flags carry NAMES ONLY, no values. We sort them for deterministic
//     fingerprinting and reject any flag containing '=' (which would hint a
//     value was leaked).
//   - RunUUID is freshly generated per run.
//   - Version and OS are static, low-entropy strings.
//   - ClusterSizeBucket is a fixed enum, never a raw topic count.
type PingPayload struct {
	Version           string            `json:"version"`
	OS                string            `json:"os"`
	Arch              string            `json:"arch"`
	Flags             []string          `json:"flags"`
	ClusterSizeBucket ClusterSizeBucket `json:"cluster_size_bucket"`
	ExitCode          int               `json:"exit_code"`
	RunUUID           string            `json:"run_uuid"`
}

// PingInput is the caller-side struct. It mirrors PingPayload but lets the
// caller pass raw inputs (e.g., topic count) that this package converts into
// bucketed values before serialization.
type PingInput struct {
	Version    string
	Flags      []string
	TopicCount int
	ExitCode   int
}

// AllowedPayloadKeys is the closed set of JSON keys a PingPayload may carry.
// Exported so the redaction test can assert that nothing else slips through.
var AllowedPayloadKeys = []string{
	"version",
	"os",
	"arch",
	"flags",
	"cluster_size_bucket",
	"exit_code",
	"run_uuid",
}

// BuildPayload materializes a PingPayload from a PingInput, sanitizing fields
// and generating a fresh anonymous RunUUID.
func BuildPayload(in PingInput) (PingPayload, error) {
	uuid, err := newRunUUID()
	if err != nil {
		return PingPayload{}, err
	}
	flags := sanitizeFlags(in.Flags)
	return PingPayload{
		Version:           strings.TrimSpace(in.Version),
		OS:                runtime.GOOS,
		Arch:              runtime.GOARCH,
		Flags:             flags,
		ClusterSizeBucket: BucketFor(in.TopicCount),
		ExitCode:          in.ExitCode,
		RunUUID:           uuid,
	}, nil
}

// sanitizeFlags returns only the flag NAMES, with no '=' or value attached.
// "--cluster=prod.yaml" → "--cluster". "--share" → "--share". Empties dropped.
// The result is sorted for stable wire output.
func sanitizeFlags(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, f := range in {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		// Strip any value half: "--cluster=prod.yaml" → "--cluster".
		if i := strings.IndexByte(f, '='); i >= 0 {
			f = f[:i]
		}
		if f == "" {
			continue
		}
		if _, ok := seen[f]; ok {
			continue
		}
		seen[f] = struct{}{}
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// newRunUUID generates an anonymous v4 UUID using crypto/rand. We hand-roll
// rather than pull a dep — the format is just 128 random bits with two fixed
// nibbles (version + variant).
func newRunUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("read random: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10xx
	h := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32]), nil
}

// Pinger sends fire-and-forget anonymous pings.
type Pinger struct {
	// Endpoint is the full URL the ping POSTs to.
	Endpoint string
	// Client overrides the default HTTP client (tests use httptest.Server).
	Client *http.Client
}

// NewPinger returns a Pinger pointed at endpoint with the default 5s timeout.
// An empty endpoint falls back to DefaultEndpoint.
func NewPinger(endpoint string) *Pinger {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = DefaultEndpoint
	}
	return &Pinger{
		Endpoint: endpoint,
		Client:   &http.Client{Timeout: PingTimeout},
	}
}

// Send posts the payload to the endpoint. Errors are returned but callers
// invoking from the audit hot path should ignore them — telemetry MUST NOT
// break a scan. Use SendAsync for true fire-and-forget.
func (p *Pinger) Send(ctx context.Context, payload PingPayload) error {
	if p == nil {
		return errors.New("nil pinger")
	}
	if err := AssertNoPII(payload); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal ping: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "kafka-attic/"+payload.Version)
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: PingTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post ping: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("ping rejected: status %d", resp.StatusCode)
	}
	return nil
}

// SendAsync schedules a ping in the background with a fresh 5s context. The
// returned channel closes when the send finishes (success or failure); most
// callers will not wait on it.
func (p *Pinger) SendAsync(payload PingPayload) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
		defer cancel()
		_ = p.Send(ctx, payload)
	}()
	return done
}

// AssertNoPII verifies a payload contains no fields outside the allowlist and
// no obvious value-bearing flag strings. It is used both as a runtime guard
// inside Send and as the spine of the redaction test.
//
// The check round-trips the payload through encoding/json so we catch any
// future field added to PingPayload that is not in AllowedPayloadKeys.
func AssertNoPII(payload PingPayload) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal for pii check: %w", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("unmarshal for pii check: %w", err)
	}
	allowed := make(map[string]struct{}, len(AllowedPayloadKeys))
	for _, k := range AllowedPayloadKeys {
		allowed[k] = struct{}{}
	}
	for k := range raw {
		if _, ok := allowed[k]; !ok {
			return fmt.Errorf("ping payload contains disallowed key %q", k)
		}
	}
	for _, f := range payload.Flags {
		if strings.ContainsRune(f, '=') {
			return fmt.Errorf("ping payload flag %q carries a value (contains '=')", f)
		}
	}
	// Defense in depth: every string field is checked for path separators or
	// '@', which would indicate a leaked file path / email.
	for _, s := range []string{payload.Version, payload.OS, payload.Arch, payload.RunUUID} {
		if strings.ContainsAny(s, "/@\\") {
			return fmt.Errorf("ping payload string %q contains path/email characters", s)
		}
	}
	return nil
}
