// Package html renders a kafka-attic Snapshot to a single-file HTML report
// with embedded CSS and JS (no external requests). Per SPEC Appendix A,
// sections appear in order: Executive summary, Verdict pie, Top candidates,
// Per-signal contribution, Flag highlights, Missing signals, Cleanup script,
// Topics omitted, Footer.
package html

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/conduktor/kafka-attic/internal/types"
)

//go:embed templates/*.html templates/*.css templates/*.js
var tmplFS embed.FS

// Config controls HTML rendering.
type Config struct {
	// Now is injectable to keep tests deterministic. Defaults to time.Now UTC.
	Now time.Time
	// ConduktorBaseURL overrides the base CTA URL (default
	// https://conduktor.io/console-insights). The UTM query params are appended
	// in Render so callers do not have to construct them.
	ConduktorBaseURL string
}

const defaultConduktorURL = "https://conduktor.io/console-insights"

// Render writes the single-file HTML report described in SPEC §3 and
// Appendix A to w.
func Render(w io.Writer, s *types.Snapshot, cfg Config) error {
	if s == nil {
		return fmt.Errorf("html.Render: nil snapshot")
	}
	if cfg.Now.IsZero() {
		cfg.Now = time.Now().UTC()
	}
	base := cfg.ConduktorBaseURL
	if base == "" {
		base = defaultConduktorURL
	}

	htmlBytes, err := tmplFS.ReadFile("templates/report.html")
	if err != nil {
		return err
	}
	cssBytes, err := tmplFS.ReadFile("templates/styles.css")
	if err != nil {
		return err
	}
	jsBytes, err := tmplFS.ReadFile("templates/script.js")
	if err != nil {
		return err
	}

	t, err := template.New("report").Parse(string(htmlBytes))
	if err != nil {
		return err
	}

	data := buildView(s, cfg, base, string(cssBytes), string(jsBytes))
	return t.Execute(w, data)
}

// view is the template data model.
type view struct {
	Cluster              types.ClusterInfo
	GeneratedAt          string
	KafkaAtticVersion    string
	AtticSpecVersion     string
	ReclaimableTB        string
	Counts               counts
	PieSlices            []pieSlice
	Rows                 []rowView
	FlagGroups           []flagGroup
	MissingSignalsNotice string
	MissingPermissions   []string
	Cleanup              cleanupView
	CleanupCTAURL        string
	FooterCTAURL         string
	CSS                  template.CSS
	JS                   template.JS
}

type counts struct {
	Total        int
	LikelyUnused int
	Candidate    int
	Inspect      int
	Active       int
}

type pieSlice struct {
	Label string
	Count int
	Color string
	Path  template.HTML // pre-rendered SVG path "d" attribute
}

type rowView struct {
	Name              string
	SlugID            string
	LastProducedLabel string
	DaysSince         string
	StorageLabel      string
	StorageBytes      string
	Score             string
	ScoreLabel        string
	VerdictLabel      string
	FlagsLabel        string
	SubRows           []subRow
	Owner             string
}

type subRow struct {
	Name     string
	Score    string
	Evidence string
	Weight   string
}

type flagGroup struct {
	Label  string
	Count  int
	Topics []string
}

type cleanupView struct {
	Included []cleanupTopic
	Omitted  []omittedTopic
}

type cleanupTopic struct {
	Name      string
	OwnerLine string
}

type omittedTopic struct {
	Name   string
	Reason string
}

func buildView(s *types.Snapshot, cfg Config, base, css, js string) view {
	v := view{
		Cluster:           s.Cluster,
		GeneratedAt:       s.GeneratedAt.UTC().Format(time.RFC3339),
		KafkaAtticVersion: s.KafkaAtticVersion,
		AtticSpecVersion:  s.AtticSpecVersion,
		CSS:               template.CSS(css), //nolint:gosec // css is a baked-in asset, not user input
		JS:                template.JS(js),   //nolint:gosec // js is a baked-in asset, not user input
		CleanupCTAURL:     utmURL(base, "cleanup-script"),
		FooterCTAURL:      utmURL(base, "footer"),
	}

	// Counts + reclaimable bytes.
	var reclaimable int64
	for _, t := range s.Topics {
		v.Counts.Total++
		switch t.Attic.Verdict {
		case types.VerdictLikelyUnused:
			v.Counts.LikelyUnused++
		case types.VerdictCandidate:
			v.Counts.Candidate++
		case types.VerdictInspect:
			v.Counts.Inspect++
		case types.VerdictActive:
			v.Counts.Active++
		}
		if t.Storage.Bytes != nil && (t.Attic.Verdict == types.VerdictLikelyUnused || t.Attic.Verdict == types.VerdictCandidate) {
			reclaimable += *t.Storage.Bytes
		}
	}
	v.ReclaimableTB = formatTB(reclaimable)

	// Pie slices.
	v.PieSlices = buildPie(v.Counts)

	// Rows.
	weights := s.Scan.ConfigSnapshot.AtticWeights
	v.Rows = buildRows(s.Topics, cfg.Now, weights)

	// Flag highlights.
	v.FlagGroups = buildFlagGroups(s.Topics)

	// Missing signals notice (conditional).
	if hasMissingSignals(s) {
		v.MissingSignalsNotice = "Some signals were unavailable for this scan. Topics affected by missing data are capped per the ATTIC evidence model."
		v.MissingPermissions = missingPermissionList(s.Scan.PermissionsObserved)
	}

	// Cleanup script.
	v.Cleanup = buildCleanup(s.Topics)

	return v
}

func formatTB(bytes int64) string {
	if bytes <= 0 {
		return "0.0"
	}
	tb := float64(bytes) / 1e12
	if tb < 0.1 {
		// show two decimals for sub-100 GB totals so the hero is not just "0.0".
		return fmt.Sprintf("%.2f", tb)
	}
	return fmt.Sprintf("%.1f", tb)
}

func buildPie(c counts) []pieSlice {
	type entry struct {
		label string
		count int
		color string
	}
	entries := []entry{
		{"Likely unused", c.LikelyUnused, "#c14545"},
		{"Candidate", c.Candidate, "#d99a2b"},
		{"Inspect", c.Inspect, "#4a89dc"},
		{"Active", c.Active, "#2da06f"},
	}
	total := 0
	for _, e := range entries {
		total += e.count
	}
	out := make([]pieSlice, 0, len(entries))
	if total == 0 {
		return out
	}
	const r = 100.0
	angle := -math.Pi / 2 // start at 12 o'clock
	// Special-case single non-zero slice: a full circle path made of two arcs.
	nonZero := 0
	for _, e := range entries {
		if e.count > 0 {
			nonZero++
		}
	}
	if nonZero == 1 {
		for _, e := range entries {
			if e.count == 0 {
				continue
			}
			// Full circle via two semicircle arcs.
			d := fmt.Sprintf("M %.4f %.4f A %.4f %.4f 0 1 1 %.4f %.4f A %.4f %.4f 0 1 1 %.4f %.4f Z",
				0.0, -r, r, r, 0.0, r, r, r, 0.0, -r)
			out = append(out, pieSlice{Label: e.label, Count: e.count, Color: e.color, Path: template.HTML(d)}) //nolint:gosec // SVG path built from numeric data; not user-controlled
		}
		return out
	}
	for _, e := range entries {
		if e.count == 0 {
			continue
		}
		frac := float64(e.count) / float64(total)
		next := angle + frac*2*math.Pi
		x1 := r * math.Cos(angle)
		y1 := r * math.Sin(angle)
		x2 := r * math.Cos(next)
		y2 := r * math.Sin(next)
		large := 0
		if frac > 0.5 {
			large = 1
		}
		d := fmt.Sprintf("M 0 0 L %.4f %.4f A %.4f %.4f 0 %d 1 %.4f %.4f Z", x1, y1, r, r, large, x2, y2)
		out = append(out, pieSlice{Label: e.label, Count: e.count, Color: e.color, Path: template.HTML(d)}) //nolint:gosec // SVG path built from numeric data; not user-controlled
		angle = next
	}
	return out
}

func buildRows(topics []types.Topic, now time.Time, weights types.AtticWeights) []rowView {
	rows := make([]rowView, 0, len(topics))
	for _, t := range topics {
		r := rowView{
			Name:              t.Name,
			SlugID:            slugify(t.Name),
			LastProducedLabel: lastProducedLabel(t.LastProduceTs, now),
			DaysSince:         daysSinceString(t.LastProduceTs, now),
			StorageLabel:      storageLabel(t),
			StorageBytes:      storageBytesValue(t),
			VerdictLabel:      verdictLabel(t),
			FlagsLabel:        flagLabels(t.Flags),
			SubRows:           subRowsFor(t, weights),
		}
		if len(t.Attic.SubScores) == 0 {
			r.Score = "-1"
			r.ScoreLabel = "—"
		} else {
			score := int(math.Round(t.Attic.RawScore))
			r.Score = fmt.Sprintf("%d", score)
			r.ScoreLabel = r.Score
		}
		if t.Owner != nil {
			r.Owner = t.Owner.Value
		}
		rows = append(rows, r)
	}
	// Pre-sort by score desc so the initial render matches "Top candidates".
	sort.SliceStable(rows, func(i, j int) bool {
		var si, sj int
		_, _ = fmt.Sscanf(rows[i].Score, "%d", &si)
		_, _ = fmt.Sscanf(rows[j].Score, "%d", &sj)
		return si > sj
	})
	return rows
}

func subRowsFor(t types.Topic, w types.AtticWeights) []subRow {
	order := []types.SubSignal{
		types.SubSignalActivity, types.SubSignalTenancy, types.SubSignalTonnage,
		types.SubSignalIntent, types.SubSignalConsumption,
	}
	weightMap := map[types.SubSignal]float64{
		types.SubSignalActivity:    w.Activity,
		types.SubSignalTenancy:     w.Tenancy,
		types.SubSignalTonnage:     w.Tonnage,
		types.SubSignalIntent:      w.Intent,
		types.SubSignalConsumption: w.Consumption,
	}
	out := make([]subRow, 0, len(order))
	for _, sig := range order {
		sub, ok := t.Attic.SubScores[sig]
		row := subRow{Name: humanSignal(sig)}
		if !ok {
			row.Score = "—"
			row.Evidence = "—"
			row.Weight = fmt.Sprintf("%.2f", weightMap[sig])
			out = append(out, row)
			continue
		}
		if sub.Skipped {
			row.Score = "skipped"
		} else {
			row.Score = fmt.Sprintf("%d", sub.Score)
		}
		row.Evidence = string(sub.Evidence)
		row.Weight = fmt.Sprintf("%.2f", weightMap[sig])
		out = append(out, row)
	}
	return out
}

func humanSignal(s types.SubSignal) string {
	switch s {
	case types.SubSignalActivity:
		return "Activity (A)"
	case types.SubSignalTenancy:
		return "Tenancy (T)"
	case types.SubSignalTonnage:
		return "Tonnage (T)"
	case types.SubSignalIntent:
		return "Intent (I)"
	case types.SubSignalConsumption:
		return "Consumption (C)"
	}
	return string(s)
}

func lastProducedLabel(ts *time.Time, now time.Time) string {
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

func daysSinceString(ts *time.Time, now time.Time) string {
	if ts == nil {
		return "100000" // never seen sorts to the top of "stale"
	}
	d := now.Sub(*ts)
	if d < 0 {
		d = 0
	}
	return fmt.Sprintf("%d", int64(d/(24*time.Hour)))
}

func storageLabel(t types.Topic) string {
	if t.Storage.Evidence == types.EvidenceUnknown || t.Storage.Bytes == nil {
		return "? GB"
	}
	return formatBytes(*t.Storage.Bytes, t.Storage.Evidence == types.EvidenceEstimated)
}

func storageBytesValue(t types.Topic) string {
	if t.Storage.Bytes == nil {
		return "-1"
	}
	return fmt.Sprintf("%d", *t.Storage.Bytes)
}

func formatBytes(b int64, estimated bool) string {
	const (
		KB = 1000.0
		MB = KB * 1000
		GB = MB * 1000
		TB = GB * 1000
	)
	f := float64(b)
	var val float64
	var unit string
	switch {
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
	var num string
	if val >= 100 {
		num = fmt.Sprintf("%.0f", val)
	} else {
		num = fmt.Sprintf("%.1f", val)
	}
	s := fmt.Sprintf("%s %s", num, unit)
	if estimated {
		s += " est"
	}
	return s
}

func verdictLabel(t types.Topic) string {
	if t.Attic.Verdict == "" || len(t.Attic.SubScores) == 0 {
		return "—"
	}
	return t.Attic.Verdict.Display()
}

func flagLabels(flags []types.Flag) string {
	if len(flags) == 0 {
		return ""
	}
	parts := make([]string, 0, len(flags))
	for _, f := range flags {
		parts = append(parts, f.Display())
	}
	return strings.Join(parts, ", ")
}

func slugify(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

// buildFlagGroups groups topics by the highlight flags listed in SPEC App A
// section 5.
func buildFlagGroups(topics []types.Topic) []flagGroup {
	order := []types.Flag{
		types.FlagOversized,
		types.FlagSkewed,
		types.FlagOrphanSchema,
		types.FlagAppearsNeverUsed,
		types.FlagPurged,
		types.FlagCompacted,
		types.FlagRemoteStorage,
	}
	out := make([]flagGroup, 0, len(order))
	for _, f := range order {
		names := []string{}
		for _, t := range topics {
			for _, tf := range t.Flags {
				if tf == f {
					names = append(names, t.Name)
					break
				}
			}
		}
		if len(names) == 0 {
			continue
		}
		out = append(out, flagGroup{Label: f.Display(), Count: len(names), Topics: names})
	}
	return out
}

func hasMissingSignals(s *types.Snapshot) bool {
	if len(s.Scan.MissingSignalsGlobal) > 0 {
		return true
	}
	p := s.Scan.PermissionsObserved
	if !p.DescribeCluster || !p.DescribeTopics || !p.DescribeConfigs || !p.DescribeGroups || !p.DescribeLogDirs {
		return true
	}
	for _, t := range s.Topics {
		if len(t.SignalsMissing) > 0 {
			return true
		}
		for _, f := range t.Flags {
			if f == types.FlagMissingSignal {
				return true
			}
		}
	}
	return false
}

func missingPermissionList(p types.PermissionsObserved) []string {
	var out []string
	if !p.DescribeCluster {
		out = append(out, "DescribeCluster denied")
	}
	if !p.DescribeTopics {
		out = append(out, "DescribeTopics denied")
	}
	if !p.DescribeConfigs {
		out = append(out, "DescribeConfigs denied")
	}
	if !p.DescribeGroups {
		out = append(out, "DescribeConsumerGroups denied")
	}
	if !p.DescribeLogDirs {
		out = append(out, "DescribeLogDirs denied (Tonnage degraded)")
	}
	if !p.SchemaRegistryRead {
		out = append(out, "Schema Registry unreachable (Intent skipped)")
	}
	return out
}

// buildCleanup applies the inclusion rules in SPEC §3.1 verbatim.
func buildCleanup(topics []types.Topic) cleanupView {
	cv := cleanupView{}
	for _, t := range topics {
		reason, included := cleanupDecision(t)
		if included {
			cv.Included = append(cv.Included, cleanupTopic{
				Name:      t.Name,
				OwnerLine: ownerLine(t.Owner),
			})
			continue
		}
		// Only list topics that scored at all and could plausibly have been
		// candidates — keep the omitted list focused on "near misses".
		if t.Attic.Verdict == "" || len(t.Attic.SubScores) == 0 {
			// Still list if the topic carries a REMOTE_STORAGE / MISSING_SIGNAL
			// flag — that is the SPEC's explicit rationale.
			if !hasAnyFlag(t.Flags, types.FlagRemoteStorage, types.FlagMissingSignal, types.FlagCompacted) {
				continue
			}
		}
		if reason == "" {
			continue
		}
		cv.Omitted = append(cv.Omitted, omittedTopic{Name: t.Name, Reason: reason})
	}
	sort.SliceStable(cv.Included, func(i, j int) bool { return cv.Included[i].Name < cv.Included[j].Name })
	sort.SliceStable(cv.Omitted, func(i, j int) bool { return cv.Omitted[i].Name < cv.Omitted[j].Name })
	return cv
}

// cleanupDecision applies §3.1 inclusion rules.
// Returns (omitReason, included). When included is true, omitReason is "".
func cleanupDecision(t types.Topic) (string, bool) {
	// Rule: verdict must be LIKELY_UNUSED.
	if t.Attic.Verdict != types.VerdictLikelyUnused {
		return "", false // not a near-miss worth listing unless flagged
	}
	// At this point the topic scored ≥ 90.
	if hasAnyFlag(t.Flags, types.FlagMissingSignal) {
		return "MISSING_SIGNAL flag present", false
	}
	if hasAnyFlag(t.Flags, types.FlagCompacted) {
		return "COMPACTED — manual review required", false
	}
	if hasAnyFlag(t.Flags, types.FlagRemoteStorage) {
		return "REMOTE_STORAGE — tiered storage state unknown", false
	}
	// All five sub-signals must have evidence KNOWN or ESTIMATED.
	for _, sig := range []types.SubSignal{
		types.SubSignalActivity, types.SubSignalTenancy, types.SubSignalTonnage,
		types.SubSignalIntent, types.SubSignalConsumption,
	} {
		sub, ok := t.Attic.SubScores[sig]
		if !ok {
			// Missing sub-signal entry → treat as UNKNOWN unless the signal is
			// explicitly skipped (Tonnage/Intent), in which case there is no
			// entry but it should not block. SPEC §3.1 says "All five ...
			// evidence KNOWN or ESTIMATED (no UNKNOWN)". A skipped signal has
			// no evidence to evaluate; conservative default: omit.
			return fmt.Sprintf("sub-signal %s missing", sig), false
		}
		if sub.Skipped {
			// SPEC is silent on skipped signals here. Conservative: keep.
			continue
		}
		if sub.Evidence == types.EvidenceUnknown {
			return fmt.Sprintf("%s evidence UNKNOWN", sig), false
		}
	}
	return "", true
}

func hasAnyFlag(flags []types.Flag, want ...types.Flag) bool {
	for _, f := range flags {
		for _, w := range want {
			if f == w {
				return true
			}
		}
	}
	return false
}

func ownerLine(o *types.OwnerInfo) string {
	if o == nil {
		return ""
	}
	if o.EntityRef != nil && *o.EntityRef != "" {
		return fmt.Sprintf("# owner: %s (%s entity: %s)", o.Value, o.Source, *o.EntityRef)
	}
	return fmt.Sprintf("# owner: %s", o.Value)
}

func utmURL(base, content string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	q := u.Query()
	q.Set("utm_source", "kafka-attic")
	q.Set("utm_medium", "oss")
	q.Set("utm_campaign", "report")
	q.Set("utm_content", content)
	u.RawQuery = q.Encode()
	return u.String()
}
