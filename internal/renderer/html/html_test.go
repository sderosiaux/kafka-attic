package html

import (
	"bytes"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/kafka-attic/internal/types"
)

var fixedNow = time.Date(2026, 5, 21, 9, 38, 0, 0, time.UTC)

func ptrInt64(v int64) *int64        { return &v }
func ptrTime(v time.Time) *time.Time { return &v }
func ptrString(v string) *string     { return &v }

// fixtureSnapshot reuses the same conceptual fixture as the terminal renderer
// tests but adds enough variation to exercise cleanup-script inclusion rules:
//   - "purge-me": LIKELY_UNUSED, all KNOWN, no excluding flags  → INCLUDED
//   - "ghost":    LIKELY_UNUSED, MISSING_SIGNAL flag             → OMITTED
//   - "compacted-state": LIKELY_UNUSED, COMPACTED                → OMITTED
//   - "remote-archive": LIKELY_UNUSED, REMOTE_STORAGE            → OMITTED
//   - "fuzzy": LIKELY_UNUSED, Activity evidence UNKNOWN          → OMITTED
//   - "audit-trail": ACTIVE, not eligible                        → ignored
func fixtureSnapshot() *types.Snapshot {
	mkSubKnown := func() map[types.SubSignal]types.SubScore {
		return map[types.SubSignal]types.SubScore{
			types.SubSignalActivity:    {Score: 100, Evidence: types.EvidenceKnown},
			types.SubSignalTenancy:     {Score: 100, Evidence: types.EvidenceKnown},
			types.SubSignalTonnage:     {Score: 95, Evidence: types.EvidenceKnown},
			types.SubSignalIntent:      {Score: 100, Evidence: types.EvidenceKnown},
			types.SubSignalConsumption: {Score: 100, Evidence: types.EvidenceKnown},
		}
	}
	mkSubActive := func() map[types.SubSignal]types.SubScore {
		return map[types.SubSignal]types.SubScore{
			types.SubSignalActivity:    {Score: 5, Evidence: types.EvidenceKnown},
			types.SubSignalTenancy:     {Score: 0, Evidence: types.EvidenceKnown},
			types.SubSignalTonnage:     {Score: 50, Evidence: types.EvidenceKnown},
			types.SubSignalIntent:      {Score: 0, Evidence: types.EvidenceKnown},
			types.SubSignalConsumption: {Score: 0, Evidence: types.EvidenceKnown},
		}
	}

	mkSubFuzzy := func() map[types.SubSignal]types.SubScore {
		m := mkSubKnown()
		s := m[types.SubSignalActivity]
		s.Evidence = types.EvidenceUnknown
		m[types.SubSignalActivity] = s
		return m
	}

	return &types.Snapshot{
		SchemaVersion:     "1.0.0",
		AtticSpecVersion:  "1.0.0",
		GeneratedAt:       fixedNow,
		KafkaAtticVersion: "1.0.0",
		Cluster: types.ClusterInfo{
			Name:                 "prod-msk",
			Bootstrap:            "b-1.msk.eu-west-1.amazonaws.com:9098",
			DetectedType:         "msk",
			KafkaVersionReported: "3.7.0",
		},
		Scan: types.ScanInfo{
			TopicCountScanned: 6,
			PermissionsObserved: types.PermissionsObserved{
				DescribeCluster: true, DescribeTopics: true, DescribeConfigs: true,
				DescribeGroups: true, DescribeLogDirs: true, SchemaRegistryRead: true,
			},
			ConfigSnapshot: types.ConfigSnapshot{
				AtticWeights: types.AtticWeights{
					Activity: 0.30, Tenancy: 0.20, Tonnage: 0.10, Intent: 0.15, Consumption: 0.25,
				},
				Thresholds: types.AtticThresholds{LikelyUnused: 90, Candidate: 70, Inspect: 40},
			},
		},
		Topics: []types.Topic{
			{
				Name:                 "purge-me",
				Partitions:           4,
				CleanupPolicy:        "delete",
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        ptrTime(fixedNow.AddDate(0, 0, -400)),
				Storage:              types.StorageInfo{Bytes: ptrInt64(123_000_000), Source: "log_dir", Evidence: types.EvidenceKnown},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0", SubScores: mkSubKnown(), RawScore: 96, Verdict: types.VerdictLikelyUnused,
				},
				Owner: &types.OwnerInfo{Value: "data-platform@acme.com", Source: "backstage", EntityRef: ptrString("component:default/orders-svc")},
			},
			{
				Name:                 "ghost",
				Partitions:           1,
				CleanupPolicy:        "delete",
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        nil,
				Storage:              types.StorageInfo{Bytes: ptrInt64(0), Source: "log_dir", Evidence: types.EvidenceKnown},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0", SubScores: mkSubKnown(), RawScore: 95, Verdict: types.VerdictLikelyUnused,
				},
				Flags: []types.Flag{types.FlagMissingSignal},
			},
			{
				Name:                 "compacted-state",
				Partitions:           4,
				CleanupPolicy:        "compact",
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        ptrTime(fixedNow.AddDate(0, 0, -120)),
				Storage:              types.StorageInfo{Bytes: ptrInt64(5_000_000_000), Source: "estimate", Evidence: types.EvidenceEstimated},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0", SubScores: mkSubKnown(), RawScore: 92, Verdict: types.VerdictLikelyUnused,
				},
				Flags: []types.Flag{types.FlagCompacted},
			},
			{
				Name:                 "remote-archive",
				Partitions:           6,
				CleanupPolicy:        "delete",
				RemoteStorageEnabled: true,
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        ptrTime(fixedNow.AddDate(0, 0, -200)),
				Storage:              types.StorageInfo{Bytes: nil, Source: "unknown", Evidence: types.EvidenceUnknown},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0", SubScores: mkSubKnown(), RawScore: 91, Verdict: types.VerdictLikelyUnused,
				},
				Flags: []types.Flag{types.FlagRemoteStorage},
			},
			{
				Name:                 "fuzzy",
				Partitions:           2,
				CleanupPolicy:        "delete",
				MessageTimestampType: "CreateTime",
				LastProduceTS:        ptrTime(fixedNow.AddDate(0, 0, -300)),
				Storage:              types.StorageInfo{Bytes: ptrInt64(100_000_000), Source: "log_dir", Evidence: types.EvidenceKnown},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0", SubScores: mkSubFuzzy(), RawScore: 90, Verdict: types.VerdictLikelyUnused,
				},
			},
			{
				Name:                 "audit-trail",
				Partitions:           3,
				CleanupPolicy:        "delete",
				MessageTimestampType: "LogAppendTime",
				LastProduceTS:        ptrTime(fixedNow.AddDate(0, 0, -1)),
				Storage:              types.StorageInfo{Bytes: ptrInt64(890_000_000), Source: "log_dir", Evidence: types.EvidenceKnown},
				Attic: types.AtticScore{
					SpecVersion: "1.0.0", SubScores: mkSubActive(), RawScore: 11, Verdict: types.VerdictActive,
				},
			},
		},
	}
}

func render(t *testing.T) string {
	t.Helper()
	var buf bytes.Buffer
	err := Render(&buf, fixtureSnapshot(), Config{Now: fixedNow})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	return buf.String()
}

func TestRender_WarningBanner(t *testing.T) {
	out := render(t)
	if !strings.Contains(out, "Permanent data deletion") {
		t.Fatal("missing warning banner phrase 'Permanent data deletion'")
	}
	if !strings.Contains(out, "Kafka has no native dry-run for topic deletion") {
		t.Fatal("missing dry-run warning sentence")
	}
}

func TestRender_InclusionRulesListed(t *testing.T) {
	out := render(t)
	required := []string{
		"Verdict is LIKELY_UNUSED",
		"No MISSING_SIGNAL flag",
		"No COMPACTED flag",
		"No REMOTE_STORAGE flag",
		"All five ATTIC sub-signals have evidence KNOWN or ESTIMATED",
	}
	for _, want := range required {
		if !strings.Contains(out, want) {
			t.Errorf("inclusion rule text missing: %q", want)
		}
	}
}

func TestRender_CleanupScriptIncludesEligibleOnly(t *testing.T) {
	out := render(t)
	// Locate the <pre class="script"> block.
	start := strings.Index(out, `<pre class="script">`)
	end := strings.Index(out, `</pre>`)
	if start < 0 || end <= start {
		t.Fatalf("script block not found")
	}
	script := out[start:end]

	if !strings.Contains(script, "kafka-topics    --bootstrap-server $BS --describe --topic purge-me") {
		t.Errorf("script missing preflight describe for purge-me")
	}
	if !strings.Contains(script, "kafka-consumer-groups --bootstrap-server $BS --describe --all-groups --topic purge-me") {
		t.Errorf("script missing describe-groups for purge-me")
	}
	if !strings.Contains(script, "kafka-topics    --bootstrap-server $BS --delete --topic purge-me") {
		t.Errorf("script missing delete for purge-me")
	}
	if !strings.Contains(script, "# owner: data-platform@acme.com") {
		t.Errorf("script missing owner annotation")
	}

	// Excluded topics must NOT appear inside the script body.
	for _, omitted := range []string{"compacted-state", "remote-archive", "ghost", "fuzzy", "audit-trail"} {
		if strings.Contains(script, "--topic "+omitted) {
			t.Errorf("script must not include excluded topic %q", omitted)
		}
	}
}

func TestRender_OmittedSectionListsExclusions(t *testing.T) {
	out := render(t)
	startIdx := strings.Index(out, `id="omitted"`)
	if startIdx < 0 {
		t.Fatal("omitted section not found")
	}
	endIdx := strings.Index(out[startIdx:], "</section>")
	if endIdx < 0 {
		t.Fatal("omitted section unterminated")
	}
	omitted := out[startIdx : startIdx+endIdx]

	mustContain := []struct {
		name, reason string
	}{
		{"ghost", "MISSING_SIGNAL"},
		{"compacted-state", "COMPACTED"},
		{"remote-archive", "REMOTE_STORAGE"},
		{"fuzzy", "UNKNOWN"},
	}
	for _, c := range mustContain {
		if !strings.Contains(omitted, c.name) {
			t.Errorf("omitted section missing topic %q", c.name)
		}
		if !strings.Contains(omitted, c.reason) {
			t.Errorf("omitted section missing reason mentioning %q", c.reason)
		}
	}
}

func TestRender_UTMTags(t *testing.T) {
	out := render(t)
	for _, want := range []string{
		"utm_source=kafka-attic",
		"utm_medium=oss",
		"utm_campaign=report",
		"utm_content=cleanup-script",
		"utm_content=footer",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("UTM tag missing in output: %q", want)
		}
	}
}

func TestRender_FooterSingleLink(t *testing.T) {
	out := render(t)
	footerStart := strings.LastIndex(out, "<footer>")
	footerEnd := strings.LastIndex(out, "</footer>")
	if footerStart < 0 || footerEnd <= footerStart {
		t.Fatal("footer not found")
	}
	footer := out[footerStart:footerEnd]
	if strings.Count(footer, "<a ") != 1 {
		t.Errorf("footer must have exactly one link, got %d", strings.Count(footer, "<a "))
	}
	if !strings.Contains(footer, "utm_content=footer") {
		t.Errorf("footer link must carry utm_content=footer")
	}
}

func TestRender_ReclaimableHero(t *testing.T) {
	out := render(t)
	if !strings.Contains(out, "RECLAIMABLE") {
		t.Errorf("hero label missing")
	}
	// purge-me (123 MB) + ghost (0) + compacted (5 GB) + remote (nil) + fuzzy (100 MB)
	// → all under 1 TB. Should render as a fractional TB string.
	if !strings.Contains(out, "TB") {
		t.Errorf("hero TB unit missing")
	}
}

func TestRender_NoExternalRequests(t *testing.T) {
	out := render(t)
	// No external script/link/img tags. The only http(s) URLs allowed are the
	// CTA hrefs.
	bad := []string{
		`<script src=`,
		`<link rel="stylesheet"`,
		`<img src="http`,
	}
	for _, s := range bad {
		if strings.Contains(out, s) {
			t.Errorf("output must be self-contained; found %q", s)
		}
	}
}

func TestRender_MissingSignalsNoticeConditional(t *testing.T) {
	// Clean snapshot: drop the topic carrying the MISSING_SIGNAL flag so the
	// notice has no reason to render.
	clean := fixtureSnapshot()
	kept := clean.Topics[:0]
	for _, top := range clean.Topics {
		skip := slices.Contains(top.Flags, types.FlagMissingSignal)
		if !skip {
			kept = append(kept, top)
		}
	}
	clean.Topics = kept
	var buf1 bytes.Buffer
	if err := Render(&buf1, clean, Config{Now: fixedNow}); err != nil {
		t.Fatalf("Render: %v", err)
	}
	if strings.Contains(buf1.String(), `id="missing-signals"`) {
		t.Errorf("missing-signals notice should be absent when all permissions present and no flagged topics")
	}

	// Now flip one permission on the original fixture.
	s := fixtureSnapshot()
	s.Scan.PermissionsObserved.DescribeLogDirs = false
	var buf bytes.Buffer
	if err := Render(&buf, s, Config{Now: fixedNow}); err != nil {
		t.Fatalf("Render: %v", err)
	}
	out2 := buf.String()
	if !strings.Contains(out2, `id="missing-signals"`) {
		t.Errorf("missing-signals notice should appear when a permission was denied")
	}
	if !strings.Contains(out2, "DescribeLogDirs") {
		t.Errorf("notice should mention DescribeLogDirs")
	}
}

func TestRender_TopCandidatesContainsAllTopics(t *testing.T) {
	out := render(t)
	for _, name := range []string{"purge-me", "ghost", "compacted-state", "remote-archive", "fuzzy", "audit-trail"} {
		if !strings.Contains(out, ">"+name+"<") {
			t.Errorf("top-candidates table missing topic %q", name)
		}
	}
}
