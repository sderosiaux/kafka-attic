package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/spf13/cobra"

	"github.com/sderosiaux/kafka-attic/internal/cluster"
	"github.com/sderosiaux/kafka-attic/internal/collector"
	"github.com/sderosiaux/kafka-attic/internal/config"
	"github.com/sderosiaux/kafka-attic/internal/renderer"
	"github.com/sderosiaux/kafka-attic/internal/scorer"
	"github.com/sderosiaux/kafka-attic/internal/telemetry"
	"github.com/sderosiaux/kafka-attic/internal/types"
)

// newScanCmd wires `kattic scan`: read-only connect → collect → score →
// render. Honors --cluster, --output, --format.
func newScanCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "scan",
		Short: "Quick read-only scan with terminal output",
		Long: `Scan a Kafka cluster and print the ATTIC Score table.

By default the output goes to stdout in the 'table' format. Use --format json
or --format csv for machine-readable output, and --output to redirect to a file.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Minute)
			defer cancel()
			snap, err := runScan(ctx, cfg)
			if err != nil {
				return err
			}
			out, closer, err := openOutput(flags.output)
			if err != nil {
				return err
			}
			defer closer()
			// When --output is unset, route through cobra's writer so tests
			// can capture the table/JSON/CSV bytes via root.SetOut.
			if flags.output == "" {
				out = cmd.OutOrStdout()
			}
			rerr := renderSnapshot(out, snap, cfg, flags.format)
			if rerr != nil {
				return rerr
			}
			pingTelemetryAsync(cfg, snap, []string{"--format", flags.format}, 0)
			return nil
		},
	}
}

// loadConfig loads the cluster YAML the user named (or the default path).
func loadConfig() (*config.Config, error) {
	return config.Load(resolveConfigPath())
}

// runScan dials the cluster, collects raw data, and runs the scorer. It is
// shared by `scan`, `audit`, and `inspect`.
func runScan(ctx context.Context, cfg *config.Config) (*types.Snapshot, error) {
	clients, err := cluster.Connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to cluster: %w", err)
	}
	defer clients.Close()

	snap, err := collector.Collect(ctx, clients, cfg)
	if err != nil {
		return nil, fmt.Errorf("collect snapshot: %w", err)
	}
	snap.KafkaAtticVersion = version

	sc := scorer.New(cfg, snap, time.Time{})
	for i := range snap.Topics {
		sc.Score(snap, &snap.Topics[i])
	}
	return snap, nil
}

// openOutput resolves an output writer. Empty path → stdout (no-op close).
func openOutput(path string) (io.Writer, func(), error) {
	if path == "" {
		return os.Stdout, func() {}, nil
	}
	f, err := os.Create(filepath.Clean(path))
	if err != nil {
		return nil, func() {}, fmt.Errorf("create output %s: %w", path, err)
	}
	return f, func() { _ = f.Close() }, nil
}

// renderSnapshot dispatches by --format. The 'table' default targets a TTY;
// JSON and CSV are intended for piping and never wrap output.
func renderSnapshot(w io.Writer, snap *types.Snapshot, cfg *config.Config, format string) error {
	switch format {
	case "", "table":
		return renderer.RenderTerminal(w, snap, renderer.TerminalOptions{})
	case "json":
		return renderer.RenderJSON(w, snap, renderer.JSONOptions{
			Redact: renderer.ResolveRedact(cfg),
		})
	case "csv":
		return renderer.RenderCSV(w, snap, renderer.CSVOptions{
			Redact: renderer.ResolveRedact(cfg),
		})
	case "html":
		// HTML from `scan` is allowed but `audit` is the documented entry point.
		// Reuse the audit pipeline so the report includes the cleanup section.
		return renderHTML(w, snap, cfg)
	default:
		return fmt.Errorf("unsupported --format %q (want one of: table, json, csv, html)", format)
	}
}

// pingTelemetryAsync fires the opt-in telemetry ping after a scan/audit. It is
// a no-op when consent is disabled or unavailable. Errors are swallowed: an
// unreachable telemetry endpoint must never affect exit status.
func pingTelemetryAsync(cfg *config.Config, snap *types.Snapshot, flagNames []string, exitCode int) {
	store, err := telemetry.DefaultStore()
	if err != nil {
		return
	}
	consent, err := store.Load()
	if err != nil || !consent.Enabled {
		return
	}
	endpoint := telemetry.DefaultEndpoint
	if cfg != nil && cfg.Telemetry != nil && cfg.Telemetry.Endpoint != "" {
		endpoint = cfg.Telemetry.Endpoint
	}
	payload, err := telemetry.BuildPayload(telemetry.PingInput{
		Version:    version,
		Flags:      flagNames,
		TopicCount: len(snap.Topics),
		ExitCode:   exitCode,
	})
	if err != nil {
		return
	}
	// Fire-and-forget; do not wait. runtime.Gosched gives the goroutine a
	// chance to start before the process exits in tight scripts.
	_ = telemetry.NewPinger(endpoint).SendAsync(payload)
	runtime.Gosched()
}
