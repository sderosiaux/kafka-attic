package history

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/conduktor/kafka-attic/internal/types"
)

// Insert persists a snapshot. It writes one row in scans (with the full JSON
// blob), and one row per topic in topic_snapshots so diff queries can read
// the minimal columns without rehydrating the blob. retentionDays > 0 prunes
// any scan whose generated_at is older than now - retentionDays in the same
// transaction, so the DB never grows unbounded.
//
// Returns the new scan ID.
func (s *Store) Insert(snap *types.Snapshot, retentionDays int) (int64, error) {
	if snap == nil {
		return 0, fmt.Errorf("history: nil snapshot")
	}
	blob, err := json.Marshal(snap)
	if err != nil {
		return 0, fmt.Errorf("history: marshal snapshot: %w", err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("history: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.Exec(
		`INSERT INTO scans (
			generated_at, cluster_name, cluster_bootstrap,
			schema_version, attic_spec_version, kafka_attic_version,
			topic_count, blob
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		snap.GeneratedAt.UTC().Format(time.RFC3339Nano),
		snap.Cluster.Name,
		snap.Cluster.Bootstrap,
		snap.SchemaVersion,
		snap.AtticSpecVersion,
		snap.KafkaAtticVersion,
		len(snap.Topics),
		blob,
	)
	if err != nil {
		return 0, fmt.Errorf("history: insert scan: %w", err)
	}
	scanID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("history: last insert id: %w", err)
	}

	stmt, err := tx.Prepare(
		`INSERT INTO topic_snapshots (
			scan_id, topic_name, verdict, raw_score,
			storage_bytes, has_bytes, cleanup_policy
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return 0, fmt.Errorf("history: prepare topic insert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, t := range snap.Topics {
		var (
			storage  sql.NullInt64
			hasBytes int
		)
		if t.Storage.Bytes != nil {
			storage = sql.NullInt64{Int64: *t.Storage.Bytes, Valid: true}
			hasBytes = 1
		}
		if _, err := stmt.Exec(
			scanID,
			t.Name,
			string(t.Attic.Verdict),
			t.Attic.RawScore,
			storage,
			hasBytes,
			t.CleanupPolicy,
		); err != nil {
			return 0, fmt.Errorf("history: insert topic %q: %w", t.Name, err)
		}
	}

	if retentionDays > 0 {
		cutoff := time.Now().UTC().Add(-time.Duration(retentionDays) * 24 * time.Hour)
		if _, err := tx.Exec(
			`DELETE FROM scans WHERE generated_at < ?`,
			cutoff.Format(time.RFC3339Nano),
		); err != nil {
			return 0, fmt.Errorf("history: prune retention: %w", err)
		}
		// topic_snapshots cleaned up via ON DELETE CASCADE.
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("history: commit: %w", err)
	}
	return scanID, nil
}

// LoadScan rehydrates a Snapshot by scan ID from the JSON blob.
func (s *Store) LoadScan(scanID int64) (*types.Snapshot, error) {
	var blob []byte
	err := s.db.QueryRow(`SELECT blob FROM scans WHERE id = ?`, scanID).Scan(&blob)
	if err == sql.ErrNoRows {
		return nil, ErrScanNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("history: load scan %d: %w", scanID, err)
	}
	var snap types.Snapshot
	if err := json.Unmarshal(blob, &snap); err != nil {
		return nil, fmt.Errorf("history: decode scan %d: %w", scanID, err)
	}
	return &snap, nil
}

// ScanCount returns the number of scans currently stored. Useful for tests
// and CLI status output.
func (s *Store) ScanCount() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM scans`).Scan(&n); err != nil {
		return 0, fmt.Errorf("history: count scans: %w", err)
	}
	return n, nil
}
