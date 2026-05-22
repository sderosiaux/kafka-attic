package renderer

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/types"
)

// JSONOptions controls JSON rendering.
type JSONOptions struct {
	// Redact applies SHA-256 redaction to sensitive identifiers when true.
	// When unset, redaction is decided from cfg.Report.RedactTopicNames.
	Redact bool
	// AnonymousRunUUID overrides the generated telemetry UUID. Empty → generate.
	AnonymousRunUUID string
	// SharedSummaryURL becomes telemetry.shared_summary_url. nil when not shared.
	SharedSummaryURL *string
}

// ResolveRedact returns whether redaction should apply given a config.
func ResolveRedact(cfg *config.Config) bool {
	if cfg == nil || cfg.Report == nil {
		return false
	}
	return cfg.Report.RedactTopicNames == "hash"
}

// RenderJSON writes the Appendix C snapshot to w, pretty-printed with 2-space
// indent. Telemetry block is always present.
func RenderJSON(w io.Writer, s *types.Snapshot, opts JSONOptions) error {
	out := cloneSnapshot(s)

	if opts.Redact {
		applyRedaction(&out)
	}

	if out.Telemetry.AnonymousRunUUID == "" {
		if opts.AnonymousRunUUID != "" {
			out.Telemetry.AnonymousRunUUID = opts.AnonymousRunUUID
		} else {
			out.Telemetry.AnonymousRunUUID = newUUID()
		}
	}
	if opts.SharedSummaryURL != nil {
		out.Telemetry.SharedSummaryURL = opts.SharedSummaryURL
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(&out)
}

// cloneSnapshot returns a deep-enough copy so renderers can mutate fields
// (redaction, telemetry) without disturbing the caller's snapshot.
func cloneSnapshot(s *types.Snapshot) types.Snapshot {
	out := *s
	out.Topics = make([]types.Topic, len(s.Topics))
	for i, t := range s.Topics {
		tc := t
		// Defensive copies of slices we may mutate during redaction.
		if t.ConsumerGroups != nil {
			tc.ConsumerGroups = append([]types.ConsumerGroupInfo(nil), t.ConsumerGroups...)
		}
		if t.SchemaRegistry != nil {
			srCopy := *t.SchemaRegistry
			if t.SchemaRegistry.SubjectsFound != nil {
				srCopy.SubjectsFound = append([]string(nil), t.SchemaRegistry.SubjectsFound...)
			}
			tc.SchemaRegistry = &srCopy
		}
		out.Topics[i] = tc
	}
	return out
}

// newUUID returns a random RFC 4122 v4 UUID string. The crypto/rand source
// avoids pulling in a third-party dependency.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Extremely rare on supported platforms. Surface a deterministic
		// fallback so JSON output remains valid.
		return "00000000-0000-4000-8000-000000000000"
	}
	// Set version (4) and variant (RFC 4122).
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	hexStr := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexStr[0:8], hexStr[8:12], hexStr[12:16], hexStr[16:20], hexStr[20:32])
}
