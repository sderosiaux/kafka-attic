// Package telemetry implements the opt-in anonymous telemetry pipeline
// described in SPEC §5.7. The package is the only place that performs
// outbound network calls outside of the Kafka collector.
//
// Three concerns are split across files:
//
//   - consent.go : persistent consent state in ~/.kattic/config.json
//   - ping.go    : anonymous fire-and-forget pings on each audit run
//   - share.go   : explicit `kattic audit --share` uploads
//
// The package follows two strict invariants:
//
//  1. Default is OFF. The first run prompts; declining is sticky.
//  2. Payloads are constructed from an allowlist of fields. Topic names,
//     consumer group names, broker addresses, schema subject names, and
//     owner data MUST NEVER be sent.
package telemetry

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ConfigFileName is the on-disk name relative to the user config dir.
const ConfigFileName = "config.json"

// ConfigDirName is the on-disk directory name relative to the user home.
const ConfigDirName = ".kattic"

// PromptText is the literal text printed during the first-run prompt.
// It is exported so tests can assert that the prompt actually fired
// without coupling to implementation details.
const PromptText = "kafka-attic can send anonymous usage telemetry (version, OS, flag names, cluster size bucket, exit code). No topic, broker, owner, or schema names ever leave your machine. Enable? [y/N]: "

// Consent captures the persisted user choice for telemetry.
//
// Prompted is true once the first-run prompt has been answered (regardless of
// the answer). It is the gate that prevents a second prompt later.
type Consent struct {
	Enabled  bool `json:"enabled"`
	Prompted bool `json:"prompted"`
}

// Store reads and writes Consent to a JSON file on disk. The zero value is
// not usable; use DefaultStore or NewStore.
type Store struct {
	// Path is the absolute path to the JSON config file.
	Path string
}

// DefaultStore returns a Store rooted at ~/.kattic/config.json.
//
// When the home directory cannot be resolved (containers with no $HOME, etc.)
// the function returns an error rather than guessing — telemetry is opt-in
// and refusing to write under an unknown path is the conservative behavior.
func DefaultStore() (*Store, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home dir: %w", err)
	}
	if strings.TrimSpace(home) == "" {
		return nil, errors.New("home directory is empty")
	}
	return &Store{Path: filepath.Join(home, ConfigDirName, ConfigFileName)}, nil
}

// NewStore builds a Store at an explicit path; used by tests.
func NewStore(path string) *Store {
	return &Store{Path: path}
}

// Load returns the persisted Consent. When the file does not exist the
// zero Consent (disabled, not yet prompted) is returned along with a nil
// error — that is the legitimate first-run state.
func (s *Store) Load() (Consent, error) {
	if s == nil || s.Path == "" {
		return Consent{}, errors.New("telemetry store path is empty")
	}
	b, err := os.ReadFile(s.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Consent{}, nil
		}
		return Consent{}, fmt.Errorf("read telemetry config %s: %w", s.Path, err)
	}
	if len(b) == 0 {
		return Consent{}, nil
	}
	var c Consent
	if err := json.Unmarshal(b, &c); err != nil {
		return Consent{}, fmt.Errorf("parse telemetry config %s: %w", s.Path, err)
	}
	return c, nil
}

// Save writes the Consent atomically (temp file + rename) with 0600 perms.
// Parent directories are created with 0700.
func (s *Store) Save(c Consent) error {
	if s == nil || s.Path == "" {
		return errors.New("telemetry store path is empty")
	}
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal consent: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".config.json.*")
	if err != nil {
		return fmt.Errorf("create temp config: %w", err)
	}
	tmpPath := tmp.Name()
	// Best-effort cleanup if anything below fails.
	defer func() { _ = os.Remove(tmpPath) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp config: %w", err)
	}
	if err := os.Rename(tmpPath, s.Path); err != nil {
		return fmt.Errorf("rename temp config: %w", err)
	}
	return nil
}

// Prompter is the interactive surface used on first run.
//
// In CI/non-interactive contexts (no TTY, no stdin) the prompt is skipped
// and consent stays at the default (disabled), Prompted=false — so the
// next interactive run can still ask.
type Prompter struct {
	// In is the reader the prompt consumes (typically os.Stdin).
	In io.Reader
	// Out is the writer the prompt prints to (typically os.Stderr).
	Out io.Writer
	// Interactive is true when stdin is a TTY. When false, EnsurePrompted
	// is a no-op so we never block a CI run waiting for input.
	Interactive bool
}

// EnsurePrompted runs the first-run prompt when Consent.Prompted is false,
// persists the answer, and returns the resulting Consent.
//
// When already prompted, it returns the existing Consent unchanged — the
// user is never re-prompted from this code path.
func EnsurePrompted(store *Store, p Prompter) (Consent, error) {
	c, err := store.Load()
	if err != nil {
		return Consent{}, err
	}
	if c.Prompted {
		return c, nil
	}
	if !p.Interactive || p.In == nil || p.Out == nil {
		// Non-interactive: leave defaults, do NOT persist Prompted=true so a
		// later interactive run can still ask.
		return c, nil
	}
	if _, err := fmt.Fprint(p.Out, PromptText); err != nil {
		return c, fmt.Errorf("write prompt: %w", err)
	}
	reader := bufio.NewReader(p.In)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return c, fmt.Errorf("read prompt response: %w", err)
	}
	ans := strings.ToLower(strings.TrimSpace(line))
	c.Prompted = true
	c.Enabled = ans == "y" || ans == "yes"
	if err := store.Save(c); err != nil {
		return c, err
	}
	return c, nil
}
