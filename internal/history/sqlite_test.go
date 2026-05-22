package history

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/conduktor/kafka-attic/internal/types"
)

func mkBytes(n int64) *int64 { return &n }

func sampleSnapshot(generatedAt time.Time, topics ...types.Topic) *types.Snapshot {
	return &types.Snapshot{
		SchemaVersion:     "1.0.0",
		AtticSpecVersion:  "1.0.0",
		GeneratedAt:       generatedAt,
		KafkaAtticVersion: "0.0.0-test",
		Cluster: types.ClusterInfo{
			Name:                 "test-cluster",
			Bootstrap:            "localhost:9092",
			DetectedType:         "apache",
			KafkaVersionReported: "3.7.0",
		},
		Topics: topics,
	}
}

func topic(name string, verdict types.Verdict, score float64, bytes *int64) types.Topic {
	return types.Topic{
		Name:          name,
		CleanupPolicy: "delete",
		Storage: types.StorageInfo{
			Bytes:    bytes,
			Source:   "log_dir",
			Evidence: types.EvidenceKnown,
		},
		Attic: types.AtticScore{
			SpecVersion: "1.0.0",
			RawScore:    score,
			Verdict:     verdict,
			SubScores:   map[types.SubSignal]types.SubScore{},
		},
	}
}

func TestStoreRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "history.db")

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	snap := sampleSnapshot(
		time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		topic("alpha", types.VerdictActive, 12.0, mkBytes(1024)),
		topic("beta", types.VerdictLikelyUnused, 95.5, mkBytes(2048)),
	)

	id, err := store.Insert(snap, 0)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if id <= 0 {
		t.Fatalf("Insert returned non-positive id: %d", id)
	}

	got, err := store.LoadScan(id)
	if err != nil {
		t.Fatalf("LoadScan: %v", err)
	}
	if got.Cluster.Name != snap.Cluster.Name {
		t.Errorf("cluster mismatch: %q vs %q", got.Cluster.Name, snap.Cluster.Name)
	}
	if len(got.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(got.Topics))
	}
	if got.Topics[1].Attic.Verdict != types.VerdictLikelyUnused {
		t.Errorf("verdict roundtrip: got %q", got.Topics[1].Attic.Verdict)
	}
	if got.Topics[1].Storage.Bytes == nil || *got.Topics[1].Storage.Bytes != 2048 {
		t.Errorf("bytes roundtrip: %+v", got.Topics[1].Storage.Bytes)
	}

	// Verify per-topic rows landed.
	var rowCount int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM topic_snapshots WHERE scan_id = ?`, id).Scan(&rowCount); err != nil {
		t.Fatalf("count topic_snapshots: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected 2 topic_snapshots rows, got %d", rowCount)
	}

	// Reopen the store and confirm persistence across handles.
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	store2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()

	n, err := store2.ScanCount()
	if err != nil {
		t.Fatalf("ScanCount: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 scan after reopen, got %d", n)
	}
}

func TestStoreInsertRetentionPrunesOldScans(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "history.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	old := sampleSnapshot(time.Now().UTC().Add(-30*24*time.Hour),
		topic("legacy", types.VerdictCandidate, 75, mkBytes(100)),
	)
	if _, err := store.Insert(old, 0); err != nil {
		t.Fatalf("Insert old: %v", err)
	}

	// Inserting a new scan with retention=7 days must prune the 30-day-old one.
	fresh := sampleSnapshot(time.Now().UTC(),
		topic("legacy", types.VerdictCandidate, 75, mkBytes(100)),
	)
	if _, err := store.Insert(fresh, 7); err != nil {
		t.Fatalf("Insert fresh: %v", err)
	}

	n, err := store.ScanCount()
	if err != nil {
		t.Fatalf("ScanCount: %v", err)
	}
	if n != 1 {
		t.Errorf("retention=7d should have pruned the 30d-old scan, got %d scans", n)
	}

	// Cascade must have removed the old topic_snapshots rows.
	var topicRows int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM topic_snapshots`).Scan(&topicRows); err != nil {
		t.Fatalf("count topic_snapshots: %v", err)
	}
	if topicRows != 1 {
		t.Errorf("expected cascade to leave 1 topic row, got %d", topicRows)
	}
}

func TestLoadScanNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "history.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	_, err = store.LoadScan(9999)
	if err != ErrScanNotFound {
		t.Errorf("expected ErrScanNotFound, got %v", err)
	}
}
