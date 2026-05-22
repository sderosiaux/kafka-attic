package telemetry

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsurePrompted_FirstRunWritesConsent(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "config.json"))

	in := strings.NewReader("y\n")
	out := &bytes.Buffer{}
	p := Prompter{In: in, Out: out, Interactive: true}

	c, err := EnsurePrompted(store, p)
	if err != nil {
		t.Fatalf("ensure prompted: %v", err)
	}
	if !c.Enabled {
		t.Fatal("expected enabled=true after y")
	}
	if !c.Prompted {
		t.Fatal("expected prompted=true after first run")
	}
	if !strings.Contains(out.String(), "Enable?") {
		t.Fatalf("prompt text not written, got: %q", out.String())
	}

	// Persisted on disk.
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !loaded.Enabled || !loaded.Prompted {
		t.Fatalf("consent not persisted: %+v", loaded)
	}
}

func TestEnsurePrompted_DeclineDefaultPersisted(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "config.json"))

	in := strings.NewReader("\n") // empty → default no
	out := &bytes.Buffer{}
	p := Prompter{In: in, Out: out, Interactive: true}

	c, err := EnsurePrompted(store, p)
	if err != nil {
		t.Fatalf("ensure prompted: %v", err)
	}
	if c.Enabled {
		t.Fatal("expected enabled=false on default decline")
	}
	if !c.Prompted {
		t.Fatal("expected prompted=true even on decline")
	}
}

func TestEnsurePrompted_NoReprompt(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "config.json"))

	// First run: accept.
	{
		in := strings.NewReader("yes\n")
		out := &bytes.Buffer{}
		p := Prompter{In: in, Out: out, Interactive: true}
		if _, err := EnsurePrompted(store, p); err != nil {
			t.Fatalf("first run: %v", err)
		}
	}

	// Second run: must NOT prompt — empty stdin and stderr-buffer should
	// remain untouched, and the persisted enabled=true must be returned.
	in := strings.NewReader("")
	out := &bytes.Buffer{}
	p := Prompter{In: in, Out: out, Interactive: true}

	c, err := EnsurePrompted(store, p)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if !c.Enabled || !c.Prompted {
		t.Fatalf("expected persisted enabled+prompted, got %+v", c)
	}
	if out.Len() != 0 {
		t.Fatalf("expected no prompt on second run, got: %q", out.String())
	}
}

func TestEnsurePrompted_NonInteractiveSkipsPrompt(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "config.json"))

	p := Prompter{Interactive: false}
	c, err := EnsurePrompted(store, p)
	if err != nil {
		t.Fatalf("non-interactive: %v", err)
	}
	if c.Enabled {
		t.Fatal("non-interactive default must be disabled")
	}
	if c.Prompted {
		t.Fatal("non-interactive must NOT mark prompted (so next interactive run can ask)")
	}
}

func TestStoreSaveLoad_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "nested", "config.json"))

	want := Consent{Enabled: true, Prompted: true}
	if err := store.Save(want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got != want {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, want)
	}
}

func TestStoreLoad_MissingFileReturnsZero(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "does-not-exist.json"))
	c, err := store.Load()
	if err != nil {
		t.Fatalf("expected nil err on missing file, got %v", err)
	}
	if c != (Consent{}) {
		t.Fatalf("expected zero consent on missing file, got %+v", c)
	}
}
