package history

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// RenderJSON writes the report as indented JSON. Useful for piping into
// downstream tools or asserting in tests.
func (r *DiffReport) RenderJSON(w io.Writer) error {
	if r == nil {
		_, err := io.WriteString(w, "null\n")
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// RenderHuman writes a terminal-friendly diff. It is deliberately plain text
// (no ANSI colour) so it stays readable in CI logs and pipes.
func (r *DiffReport) RenderHuman(w io.Writer) error {
	if r == nil {
		_, err := fmt.Fprintln(w, "(no diff)")
		return err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Diff %s -> %s\n", r.A.GeneratedAt, r.B.GeneratedAt)
	if r.A.Cluster != "" || r.B.Cluster != "" {
		fmt.Fprintf(&b, "Cluster: %s -> %s\n", coalesce(r.A.Cluster), coalesce(r.B.Cluster))
	}
	fmt.Fprintf(&b, "Topics: %d -> %d\n", r.A.TopicCount, r.B.TopicCount)
	fmt.Fprintln(&b)

	fmt.Fprintf(&b, "Newly LIKELY_UNUSED (%d)\n", len(r.NewlyLikelyUnused))
	writeDeltas(&b, r.NewlyLikelyUnused)
	fmt.Fprintln(&b)

	fmt.Fprintf(&b, "Regressions (%d)\n", len(r.Regressions))
	writeDeltas(&b, r.Regressions)
	fmt.Fprintln(&b)

	fmt.Fprintf(&b, "Deletions (%d)\n", len(r.Deletions))
	writeDeltas(&b, r.Deletions)
	fmt.Fprintln(&b)

	fmt.Fprintf(&b, "Reclaimed bytes: %s\n", humanBytes(r.ReclaimedBytes))
	if n := len(r.ReclaimedUnknownTopics); n > 0 {
		fmt.Fprintf(&b, "Reclaimed bytes excludes %d topic(s) with unknown prior storage:\n", n)
		for _, name := range r.ReclaimedUnknownTopics {
			fmt.Fprintf(&b, "  - %s\n", name)
		}
	}

	_, err := io.WriteString(w, b.String())
	return err
}

func writeDeltas(w io.Writer, deltas []TopicDelta) {
	if len(deltas) == 0 {
		fmt.Fprintln(w, "  (none)")
		return
	}
	for _, d := range deltas {
		fmt.Fprintf(w, "  - %s", d.Name)
		switch {
		case d.BeforeVerdict != "" && d.AfterVerdict != "":
			fmt.Fprintf(w, ": %s -> %s", d.BeforeVerdict, d.AfterVerdict)
		case d.AfterVerdict != "":
			fmt.Fprintf(w, ": (new) -> %s", d.AfterVerdict)
		case d.BeforeVerdict != "":
			fmt.Fprintf(w, ": %s -> (deleted)", d.BeforeVerdict)
		}
		if d.BeforeBytes != nil || d.AfterBytes != nil {
			fmt.Fprintf(w, " [%s -> %s]", bytesLabel(d.BeforeBytes), bytesLabel(d.AfterBytes))
		}
		fmt.Fprintln(w)
	}
}

func bytesLabel(b *int64) string {
	if b == nil {
		return "?"
	}
	return humanBytes(*b)
}

func coalesce(s string) string {
	if s == "" {
		return "(unknown)"
	}
	return s
}

// humanBytes renders a byte count in IEC units (KiB, MiB, ...). 0 is "0 B".
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	suffixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	if exp >= len(suffixes) {
		exp = len(suffixes) - 1
	}
	return fmt.Sprintf("%.2f %s", float64(n)/float64(div), suffixes[exp])
}
