// Package history persists past scan snapshots into a local SQLite database
// (pure-Go modernc.org/sqlite driver, no CGO) and computes diffs between two
// recorded scans. History is optional and only powers `kattic diff` and the
// HTML report's trend charts. ATTIC scoring is determined from a single scan
// and never depends on this package.
package history

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// DefaultPath returns the conventional location for the history DB
// (~/.kattic/history.db). It expands ~ relative to the current user's
// home directory.
func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("history: resolve home dir: %w", err)
	}
	return filepath.Join(home, ".kattic", "history.db"), nil
}

// Store is a thin wrapper around a *sql.DB connected to the kattic history
// database. It owns the schema lifecycle (create-if-missing) and exposes
// the Insert/Diff/Prune operations consumed by the CLI.
type Store struct {
	db   *sql.DB
	path string
}

// Open connects to the SQLite database at path, creating the file and
// parent directory if they do not exist, and ensures the schema is in
// place. An empty path falls back to DefaultPath().
func Open(path string) (*Store, error) {
	if path == "" {
		p, err := DefaultPath()
		if err != nil {
			return nil, err
		}
		path = p
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("history: mkdir %s: %w", dir, err)
		}
	}
	// _pragma=busy_timeout helps tests and concurrent runs.
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("history: open %s: %w", path, err)
	}
	// Verify the connection eagerly so callers get a clear error.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("history: ping %s: %w", path, err)
	}
	s := &Store{db: db, path: path}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Path returns the file path backing this store.
func (s *Store) Path() string { return s.path }

// DB exposes the underlying *sql.DB. Intended for tests and tooling only.
func (s *Store) DB() *sql.DB { return s.db }

// migrate creates the schema if it does not exist. The schema is intentionally
// minimal: scans store the canonical JSON blob, topic_snapshots stores the
// per-topic columns needed by Diff so we never have to re-parse the blob for
// diff queries.
func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS scans (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			generated_at    TEXT    NOT NULL,
			cluster_name    TEXT    NOT NULL,
			cluster_bootstrap TEXT  NOT NULL,
			schema_version  TEXT    NOT NULL,
			attic_spec_version TEXT NOT NULL,
			kafka_attic_version TEXT NOT NULL,
			topic_count     INTEGER NOT NULL,
			blob            BLOB    NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_scans_generated_at ON scans(generated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_scans_cluster ON scans(cluster_name)`,
		`CREATE TABLE IF NOT EXISTS topic_snapshots (
			scan_id        INTEGER NOT NULL,
			topic_name     TEXT    NOT NULL,
			verdict        TEXT    NOT NULL,
			raw_score      REAL    NOT NULL,
			storage_bytes  INTEGER,
			has_bytes      INTEGER NOT NULL DEFAULT 0,
			cleanup_policy TEXT,
			PRIMARY KEY (scan_id, topic_name),
			FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_topic_snapshots_topic ON topic_snapshots(topic_name)`,
		`CREATE INDEX IF NOT EXISTS idx_topic_snapshots_verdict ON topic_snapshots(verdict)`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("history: migrate (%q): %w", q, err)
		}
	}
	return nil
}

// ErrScanNotFound is returned when a scan ID is not present in the store.
var ErrScanNotFound = errors.New("history: scan not found")
