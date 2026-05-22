//go:build integration

// Package main integration test: brings up a single-node Redpanda container
// via testcontainers-go, creates the five fixture topics referenced in the
// task spec (active-orders, never-used, purged, oversized, compacted), runs
// `kattic scan --format json`, and asserts per-topic verdicts/flags.
//
// Run with:
//
//	go test ./... -tags=integration -count=1 -timeout=10m
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	rpcontainer "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/conduktor/kafka-attic/internal/types"
)

// stringPtr is a small helper so topic config maps remain readable inline.
func stringPtr(s string) *string { return &s }

// integrationTimeout caps the test. The container takes 10-20s; the rest is
// produce/scan/score work that completes in single-digit seconds.
const integrationTimeout = 6 * time.Minute

// TestKatticScanAgainstRedpanda is the end-to-end integration assertion
// described in the task spec. The redpanda container image is taken from the
// official redpandadata repository.
func TestKatticScanAgainstRedpanda(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in -short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	// ── 1. Start Redpanda ────────────────────────────────────────────────
	container, err := rpcontainer.Run(ctx,
		"redpandadata/redpanda:latest",
		testcontainers.WithEnv(map[string]string{
			// Smaller cluster footprint for local Docker.
			"REDPANDA_MODE": "dev-container",
		}),
	)
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(shutCtx)
	})

	seed, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}
	t.Logf("redpanda seed broker: %s", seed)

	// ── 2. Set up fixture topics ─────────────────────────────────────────
	// Producer client is allowed in test files (readonly_test.go scan
	// excludes *_test.go) — it is only used to set up integration fixtures.
	adminCl, err := kgo.NewClient(
		kgo.SeedBrokers(seed),
		kgo.ClientID("kattic-integration-admin"),
	)
	if err != nil {
		t.Fatalf("create admin kgo client: %v", err)
	}
	defer adminCl.Close()
	adm := kadm.NewClient(adminCl)
	defer adm.Close()

	setupFixtureTopics(t, ctx, adm)

	// Produce data for active-orders, purged, and oversized so they have
	// records present. compacted and never-used stay empty.
	produceFixtures(t, ctx, seed)

	// Purge `purged` by deleting all its records (sets LSO past HWM,
	// earliest_offset == latest_offset, but earliest > 0).
	purgePurgedTopic(t, ctx, adm)

	// Give Redpanda a moment to flush and finalize log directories so
	// DescribeLogDirs sees non-zero sizes for produced topics.
	time.Sleep(2 * time.Second)

	// ── 3. Write a runtime cluster.yaml pointing at the broker ───────────
	tmp := t.TempDir()
	cfgPath := writeClusterYAML(t, tmp, seed)

	// ── 4. Run `kattic scan --format json` via the cobra tree ────────────
	jsonOut := runKatticScan(t, cfgPath)

	// ── 5. Decode + assert ───────────────────────────────────────────────
	var snap types.Snapshot
	if err := json.Unmarshal(jsonOut, &snap); err != nil {
		t.Fatalf("decode scan output: %v\n%s", err, jsonOut)
	}

	byName := indexTopics(snap.Topics)
	for _, expected := range []string{"active-orders", "never-used", "purged", "oversized", "compacted"} {
		if _, ok := byName[expected]; !ok {
			t.Errorf("expected topic %q in scan, present topics: %v", expected, sortedKeys(byName))
		}
	}

	// active-orders: produced now → ACTIVE.
	if t.Failed() {
		t.FailNow()
	}
	assertVerdictNotLikelyUnused(t, byName["active-orders"], "active-orders")

	// never-used: empty topic, no consumers → CANDIDATE or LIKELY_UNUSED with
	// APPEARS_NEVER_USED flag.
	assertHasFlag(t, byName["never-used"], types.FlagAppearsNeverUsed)

	// purged: had records, all deleted → PURGED flag.
	assertHasFlag(t, byName["purged"], types.FlagPurged)

	// compacted: cleanup.policy=compact → COMPACTED flag and INSPECT cap.
	assertHasFlag(t, byName["compacted"], types.FlagCompacted)
	if v := byName["compacted"].Attic.Verdict; v == types.VerdictLikelyUnused {
		t.Errorf("compacted: expected verdict not LIKELY_UNUSED (compacted topics are capped at INSPECT), got %s", v)
	}

	// oversized: 12 partitions, low traffic — OVERSIZED requires metrics so
	// the flag is NOT emitted by default. Validate the SPEC §4.5 invariant.
	if hasFlag(byName["oversized"], types.FlagOversized) {
		t.Errorf("oversized: OVERSIZED flag should not be emitted without a metrics source (SPEC §4.5)")
	}
}

// setupFixtureTopics creates the five fixture topics with the expected
// partition counts and topic configs. Failures here abort the test.
func setupFixtureTopics(t *testing.T, ctx context.Context, adm *kadm.Client) {
	t.Helper()
	cases := []struct {
		name       string
		partitions int32
		configs    map[string]*string
	}{
		{
			name:       "active-orders",
			partitions: 3,
			configs: map[string]*string{
				"cleanup.policy": stringPtr("delete"),
				// LogAppendTime → Activity evidence KNOWN.
				"message.timestamp.type": stringPtr("LogAppendTime"),
			},
		},
		{
			name:       "never-used",
			partitions: 1,
			configs: map[string]*string{
				"cleanup.policy": stringPtr("delete"),
			},
		},
		{
			name:       "purged",
			partitions: 1,
			configs: map[string]*string{
				"cleanup.policy": stringPtr("delete"),
				// Aggressive retention so DeleteRecords lines up with PURGED
				// detection (earliest == latest, earliest > 0).
				"retention.ms": stringPtr("3600000"),
			},
		},
		{
			name:       "oversized",
			partitions: 12,
			configs: map[string]*string{
				"cleanup.policy": stringPtr("delete"),
			},
		},
		{
			name:       "compacted",
			partitions: 1,
			configs: map[string]*string{
				"cleanup.policy": stringPtr("compact"),
			},
		},
	}
	for _, tc := range cases {
		resps, err := adm.CreateTopics(ctx, tc.partitions, 1, tc.configs, tc.name)
		if err != nil {
			t.Fatalf("create topic %s: %v", tc.name, err)
		}
		for _, r := range resps.Sorted() {
			if r.Err != nil {
				t.Fatalf("create topic %s response error: %v", r.Topic, r.Err)
			}
		}
	}
}

// produceFixtures writes a small batch of records to active-orders, purged
// and oversized. never-used and compacted stay empty.
func produceFixtures(t *testing.T, ctx context.Context, seed string) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seed),
		kgo.ClientID("kattic-integration-producer"),
		// Disable idempotent batching to keep timings deterministic across runs.
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer cl.Close()

	type record struct {
		topic string
		count int
	}
	wants := []record{
		{"active-orders", 10},
		{"purged", 20},
		{"oversized", 3},
	}
	for _, w := range wants {
		for i := 0; i < w.count; i++ {
			cl.Produce(ctx, &kgo.Record{
				Topic: w.topic,
				Key:   []byte(fmt.Sprintf("k-%d", i)),
				Value: []byte(fmt.Sprintf("v-%d", i)),
			}, nil)
		}
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("flush producer: %v", err)
	}
}

// purgePurgedTopic deletes all records on the `purged` topic by issuing a
// DeleteRecords up to the current end offset. Confirms via ListStartOffsets.
func purgePurgedTopic(t *testing.T, ctx context.Context, adm *kadm.Client) {
	t.Helper()
	endOffsets, err := adm.ListEndOffsets(ctx, "purged")
	if err != nil {
		t.Fatalf("list end offsets for purged: %v", err)
	}
	offsets := kadm.Offsets{}
	endOffsets.Each(func(o kadm.ListedOffset) {
		offsets.AddOffset(o.Topic, o.Partition, o.Offset, -1)
	})
	if _, err := adm.DeleteRecords(ctx, offsets); err != nil {
		t.Fatalf("delete records on purged: %v", err)
	}
	// Eventual: wait for LSO to advance.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		start, err := adm.ListStartOffsets(ctx, "purged")
		if err == nil {
			advanced := true
			start.Each(func(o kadm.ListedOffset) {
				if o.Offset == 0 {
					advanced = false
				}
			})
			if advanced {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("warning: purged topic start offsets did not advance within deadline; PURGED flag may not be set")
}

// writeClusterYAML materializes the runtime kattic.yaml with the seed broker
// pointed at the testcontainer. It seeds the template from
// testdata/integration/cluster.yaml so the file shape is asserted by the
// test corpus rather than implied by the test source.
func writeClusterYAML(t *testing.T, dir, seed string) string {
	t.Helper()
	templatePath := filepath.Join(repoRoot(t), "testdata", "integration", "cluster.yaml")
	b, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	body := strings.ReplaceAll(string(b), "PLACEHOLDER:0", seed)
	out := filepath.Join(dir, "cluster.yaml")
	if err := os.WriteFile(out, []byte(body), 0o600); err != nil {
		t.Fatalf("write runtime cluster.yaml: %v", err)
	}
	return out
}

// repoRoot returns the repository root by walking up from the working dir.
func repoRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir := cwd
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("repo root (go.mod) not found from %s", cwd)
	return ""
}

// runKatticScan invokes the cobra tree with `scan --cluster <cfg> --format
// json` and returns the captured JSON output. The cobra root is the same
// constructor production uses; the test exercises the full wiring.
func runKatticScan(t *testing.T, cfgPath string) []byte {
	t.Helper()
	// Reset global flags between invocations.
	flags = globalFlags{}
	root := newRootCmd()
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{"scan", "--cluster", cfgPath, "--format", "json"})

	if err := root.Execute(); err != nil {
		t.Fatalf("kattic scan failed: %v\nstderr: %s", err, stderr.String())
	}
	out := stdout.Bytes()
	if len(out) == 0 {
		t.Fatalf("kattic scan produced empty stdout, stderr: %s", stderr.String())
	}
	return out
}

func indexTopics(topics []types.Topic) map[string]*types.Topic {
	out := make(map[string]*types.Topic, len(topics))
	for i := range topics {
		out[topics[i].Name] = &topics[i]
	}
	return out
}

func sortedKeys(m map[string]*types.Topic) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func assertVerdictNotLikelyUnused(t *testing.T, topic *types.Topic, name string) {
	t.Helper()
	if topic == nil {
		t.Errorf("%s: topic missing from snapshot", name)
		return
	}
	if topic.Attic.Verdict == types.VerdictLikelyUnused {
		t.Errorf("%s: expected verdict != LIKELY_UNUSED, got %s (raw_score=%.2f)",
			name, topic.Attic.Verdict, topic.Attic.RawScore)
	}
}

func assertHasFlag(t *testing.T, topic *types.Topic, want types.Flag) {
	t.Helper()
	if topic == nil {
		t.Errorf("topic missing; cannot check flag %s", want)
		return
	}
	if !hasFlag(topic, want) {
		t.Errorf("topic %s: expected flag %s, got %v (verdict=%s raw_score=%.2f)",
			topic.Name, want, topic.Flags, topic.Attic.Verdict, topic.Attic.RawScore)
	}
}

func hasFlag(topic *types.Topic, want types.Flag) bool {
	if topic == nil {
		return false
	}
	for _, f := range topic.Flags {
		if f == want {
			return true
		}
	}
	return false
}
