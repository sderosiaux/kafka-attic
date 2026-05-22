// Package renderer turns a *types.Snapshot into terminal, JSON, and CSV
// outputs as specified in SPEC §3 and Appendix C.
package renderer

import (
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/conduktor/kafka-attic/internal/types"
)

// TerminalOptions controls terminal rendering. Now is injectable to keep
// tests deterministic.
type TerminalOptions struct {
	Now time.Time
}

// RenderTerminal writes the human terminal table described in SPEC §3 to w.
// Machine enums are never printed; Verdict.Display and Flag.Display are the
// only sources of user-facing labels.
func RenderTerminal(w io.Writer, s *types.Snapshot, opts TerminalOptions) error {
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}

	totalTopics := len(s.Topics)
	likelyUnused := 0
	var reclaimable int64
	for _, t := range s.Topics {
		if t.Attic.Verdict == types.VerdictLikelyUnused {
			likelyUnused++
		}
		if t.Storage.Bytes != nil && (t.Attic.Verdict == types.VerdictLikelyUnused || t.Attic.Verdict == types.VerdictCandidate) {
			reclaimable += *t.Storage.Bytes
		}
	}

	if _, err := fmt.Fprintf(w, "%s topics · %s likely unused · %s reclaimable\n\n",
		formatCount(totalTopics), formatCount(likelyUnused), formatStorageBytes(&reclaimable, false)); err != nil {
		return err
	}

	rows := make([][6]string, 0, len(s.Topics))
	for _, t := range s.Topics {
		rows = append(rows, [6]string{
			t.Name,
			formatLastProduced(t.LastProduceTs, opts.Now),
			formatTopicStorage(t),
			formatScore(t),
			formatVerdict(t),
			formatNotes(t),
		})
	}

	headers := [6]string{"TOPIC", "LAST PRODUCED", "STORAGE", "SCORE", "VERDICT", "NOTES"}
	widths := [6]int{}
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if l := runeLen(c); l > widths[i] {
				widths[i] = l
			}
		}
	}

	// Header line.
	if err := writeRow(w, headers, widths); err != nil {
		return err
	}
	// Separator with U+2500 box-drawing dashes per SPEC sample.
	sep := [6]string{
		strings.Repeat("─", widths[0]),
		strings.Repeat("─", widths[1]),
		strings.Repeat("─", widths[2]),
		strings.Repeat("─", widths[3]),
		strings.Repeat("─", widths[4]),
		strings.Repeat("─", widths[5]),
	}
	if err := writeRow(w, sep, widths); err != nil {
		return err
	}
	for _, r := range rows {
		if err := writeRow(w, r, widths); err != nil {
			return err
		}
	}
	return nil
}

func writeRow(w io.Writer, cells [6]string, widths [6]int) error {
	parts := make([]string, 6)
	for i, c := range cells {
		parts[i] = padRight(c, widths[i])
	}
	// Trim trailing spaces on the last column.
	parts[5] = strings.TrimRight(parts[5], " ")
	_, err := fmt.Fprintln(w, strings.Join(parts, "  "))
	return err
}

func padRight(s string, w int) string {
	n := runeLen(s)
	if n >= w {
		return s
	}
	return s + strings.Repeat(" ", w-n)
}

func runeLen(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}

// formatCount formats an integer with thousand separators (comma) to match
// the SPEC sample: "4,821 topics".
func formatCount(n int) string {
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		return "-" + formatCount(-n)
	}
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	rem := len(s) % 3
	if rem > 0 {
		b.WriteString(s[:rem])
		if len(s) > rem {
			b.WriteByte(',')
		}
	}
	for i := rem; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	return b.String()
}

// formatLastProduced returns "287d ago", "2h ago", "never seen".
func formatLastProduced(ts *time.Time, now time.Time) string {
	if ts == nil {
		return "never seen"
	}
	d := now.Sub(*ts)
	if d < 0 {
		d = 0
	}
	days := int64(d / (24 * time.Hour))
	if days >= 1 {
		return fmt.Sprintf("%dd ago", days)
	}
	hours := int64(d / time.Hour)
	if hours >= 1 {
		return fmt.Sprintf("%dh ago", hours)
	}
	mins := int64(d / time.Minute)
	if mins >= 1 {
		return fmt.Sprintf("%dm ago", mins)
	}
	return "just now"
}

// formatTopicStorage renders the STORAGE cell with the " est" suffix or "?"
// per SPEC §3.
func formatTopicStorage(t types.Topic) string {
	if t.Storage.Evidence == types.EvidenceUnknown || t.Storage.Bytes == nil {
		return "? GB"
	}
	return formatStorageBytes(t.Storage.Bytes, t.Storage.Evidence == types.EvidenceEstimated)
}

// formatStorageBytes formats a byte count as "12.3 GB" / "890 MB" / "0 B".
// When estimated, " est" is appended.
func formatStorageBytes(bytes *int64, estimated bool) string {
	if bytes == nil {
		s := "? GB"
		if estimated {
			s += " est"
		}
		return s
	}
	b := *bytes
	const (
		KB = 1000.0
		MB = KB * 1000
		GB = MB * 1000
		TB = GB * 1000
		PB = TB * 1000
	)
	f := float64(b)
	var (
		val  float64
		unit string
	)
	switch {
	case f >= PB:
		val, unit = f/PB, "PB"
	case f >= TB:
		val, unit = f/TB, "TB"
	case f >= GB:
		val, unit = f/GB, "GB"
	case f >= MB:
		val, unit = f/MB, "MB"
	case f >= KB:
		val, unit = f/KB, "KB"
	default:
		s := fmt.Sprintf("%d B", b)
		if estimated {
			s += " est"
		}
		return s
	}
	// 1 decimal place, but drop ".0" only when the value is a whole number
	// AND >= 100 (matches "412 GB" sample). Otherwise keep 1 decimal.
	var num string
	switch {
	case val >= 100 && val == math.Trunc(val):
		num = fmt.Sprintf("%d", int64(val))
	case val >= 100:
		num = fmt.Sprintf("%.0f", val)
	default:
		num = fmt.Sprintf("%.1f", val)
	}
	s := fmt.Sprintf("%s %s", num, unit)
	if estimated {
		s += " est"
	}
	return s
}

// formatScore returns the SCORE cell. "—" when the verdict itself is missing
// (REMOTE_STORAGE topics with no usable score per the SPEC sample row
// "remote-archive ... ? GB — —").
func formatScore(t types.Topic) string {
	if t.Attic.Verdict == "" {
		return "—"
	}
	// Per SPEC sample, REMOTE_STORAGE rows with no usable score display "—".
	// We treat that as: no sub-scores collected at all.
	if len(t.Attic.SubScores) == 0 {
		return "—"
	}
	return fmt.Sprintf("%d", int64(math.Round(t.Attic.RawScore)))
}

func formatVerdict(t types.Topic) string {
	if t.Attic.Verdict == "" {
		return "—"
	}
	if len(t.Attic.SubScores) == 0 {
		return "—"
	}
	return t.Attic.Verdict.Display()
}

// formatNotes joins flag display labels with ", ". Empty when no flags.
func formatNotes(t types.Topic) string {
	if len(t.Flags) == 0 {
		return ""
	}
	parts := make([]string, 0, len(t.Flags))
	for _, f := range t.Flags {
		parts = append(parts, f.Display())
	}
	return strings.Join(parts, ", ")
}
