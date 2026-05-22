package renderer

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var updateGolden = flag.Bool("update", false, "rewrite golden files")

func TestRenderTerminalGolden(t *testing.T) {
	snap := fixtureSnapshot()
	var buf bytes.Buffer
	if err := RenderTerminal(&buf, snap, TerminalOptions{Now: fixedNow}); err != nil {
		t.Fatalf("render: %v", err)
	}

	goldenPath := filepath.Join("testdata", "terminal.golden")
	if *updateGolden {
		err := os.WriteFile(goldenPath, buf.Bytes(), 0o644)
		if err != nil {
			t.Fatalf("write golden: %v", err)
		}
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (run with -update first?): %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("terminal output mismatch.\n--- got ---\n%s\n--- want ---\n%s", buf.String(), string(want))
	}
}

func TestFormatLastProduced(t *testing.T) {
	cases := []struct {
		offset string
		want   string
	}{
		{"-287d", "287d ago"},
		{"-2h", "2h ago"},
		{"never", "never seen"},
	}
	for _, c := range cases {
		var s string
		switch c.offset {
		case "never":
			s = formatLastProduced(nil, fixedNow)
		case "-287d":
			ts := fixedNow.AddDate(0, 0, -287)
			s = formatLastProduced(&ts, fixedNow)
		case "-2h":
			ts := fixedNow.Add(-2 * 3600_000_000_000)
			s = formatLastProduced(&ts, fixedNow)
		}
		if s != c.want {
			t.Errorf("%s: got %q want %q", c.offset, s, c.want)
		}
	}
}

func TestFormatStorageBytes(t *testing.T) {
	cases := []struct {
		bytes     int64
		estimated bool
		want      string
	}{
		{0, false, "0 B"},
		{890_000_000, false, "890 MB"},
		{2_100_000_000, false, "2.1 GB"},
		{12_300_000_000, false, "12.3 GB"},
		{412_000_000_000, false, "412 GB"},
		{5_200_000_000, true, "5.2 GB est"},
	}
	for _, c := range cases {
		b := c.bytes
		got := formatStorageBytes(&b, c.estimated)
		if got != c.want {
			t.Errorf("%d est=%v: got %q want %q", c.bytes, c.estimated, got, c.want)
		}
	}
}
