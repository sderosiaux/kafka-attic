package owners

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeOwnersFile(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "owners.yaml")
	err := os.WriteFile(p, []byte(contents), 0o644)
	if err != nil {
		t.Fatalf("write owners.yaml: %v", err)
	}
	return p
}

func TestFileSource_FirstMatchWins(t *testing.T) {
	path := writeOwnersFile(t, `
- pattern: '^orders-.*'
  owner: team-orders@acme.com
- pattern: '^orders-experiments-.*'
  owner: team-labs@acme.com
- pattern: '.*'
  owner: catch-all@acme.com
`)
	src, warns, err := newFileSource(path)
	if err != nil {
		t.Fatalf("newFileSource: %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}

	owner, err := src.Lookup(context.Background(), "orders-experiments-foo", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner == nil {
		t.Fatal("expected match for orders-experiments-foo")
	}
	if owner.Value != "team-orders@acme.com" {
		t.Errorf("first-match wins violated: got %q", owner.Value)
	}
	if owner.Source != SourceFile {
		t.Errorf("source mismatch: got %q", owner.Source)
	}
}

func TestFileSource_NoMatchReturnsNil(t *testing.T) {
	path := writeOwnersFile(t, `
- pattern: '^orders-.*'
  owner: team-orders@acme.com
`)
	src, _, err := newFileSource(path)
	if err != nil {
		t.Fatalf("newFileSource: %v", err)
	}
	owner, err := src.Lookup(context.Background(), "payments-foo", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner != nil {
		t.Fatalf("expected nil owner, got %+v", owner)
	}
}

func TestFileSource_InvalidRegexLoggedAndSkipped(t *testing.T) {
	path := writeOwnersFile(t, `
- pattern: '['
  owner: team-broken@acme.com
- pattern: '^orders-.*'
  owner: team-orders@acme.com
`)
	src, warns, err := newFileSource(path)
	if err != nil {
		t.Fatalf("newFileSource: %v", err)
	}
	if len(warns) != 1 {
		t.Fatalf("expected one warning, got %d: %v", len(warns), warns)
	}
	if !strings.Contains(warns[0], "invalid pattern") {
		t.Errorf("warning text unexpected: %q", warns[0])
	}
	owner, err := src.Lookup(context.Background(), "orders-foo", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner == nil || owner.Value != "team-orders@acme.com" {
		t.Fatalf("expected orders match after skipping invalid regex, got %+v", owner)
	}
}

func TestFileSource_WrappedEntriesKey(t *testing.T) {
	path := writeOwnersFile(t, `
entries:
  - pattern: '^payments-.*'
    owner: team-pay@acme.com
`)
	src, _, err := newFileSource(path)
	if err != nil {
		t.Fatalf("newFileSource: %v", err)
	}
	owner, _ := src.Lookup(context.Background(), "payments-1", nil)
	if owner == nil || owner.Value != "team-pay@acme.com" {
		t.Fatalf("expected wrapped entries to parse, got %+v", owner)
	}
}

func TestFileSource_EmptyOwnerFallsThrough(t *testing.T) {
	path := writeOwnersFile(t, `
- pattern: '^orders-.*'
  owner: ''
`)
	src, _, err := newFileSource(path)
	if err != nil {
		t.Fatalf("newFileSource: %v", err)
	}
	owner, err := src.Lookup(context.Background(), "orders-1", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner != nil {
		t.Fatalf("expected nil for empty owner, got %+v", owner)
	}
}
