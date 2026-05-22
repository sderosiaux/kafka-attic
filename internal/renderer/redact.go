package renderer

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

// sha256Hex returns the lowercase hex SHA-256 of s. Stable across runs and
// processes — the redaction is deterministic.
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// applyRedaction rewrites identifiers that may encode sensitive information
// (customer IDs, jurisdictions) in place on the supplied snapshot. The HTML
// renderer never invokes this — only JSON/CSV/shared artifacts redact.
//
// Per SPEC §5.6, redaction applies to:
//   - topic names
//   - consumer group names
//   - Schema Registry subject names
func applyRedaction(s *types.Snapshot) {
	for i := range s.Topics {
		t := &s.Topics[i]
		hashed := sha256Hex(t.Name)
		t.Name = hashed
		// name_redacted reflects the same hash so downstream consumers know
		// the snapshot is redacted.
		nr := hashed
		t.NameRedacted = &nr

		for j := range t.ConsumerGroups {
			t.ConsumerGroups[j].GroupID = sha256Hex(t.ConsumerGroups[j].GroupID)
		}
		if t.SchemaRegistry != nil {
			for k, subj := range t.SchemaRegistry.SubjectsFound {
				t.SchemaRegistry.SubjectsFound[k] = sha256Hex(subj)
			}
		}
	}
}
