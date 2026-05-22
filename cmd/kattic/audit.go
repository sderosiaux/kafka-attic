package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/sderosiaux/kafka-attic/internal/config"
	"github.com/sderosiaux/kafka-attic/internal/history"
	"github.com/sderosiaux/kafka-attic/internal/renderer/html"
	"github.com/sderosiaux/kafka-attic/internal/telemetry"
	"github.com/sderosiaux/kafka-attic/internal/types"
)

// defaultAuditOutput is the HTML report path used when --output is not set.
const defaultAuditOutput = "./attic-report.html"

// newAuditCmd wires `kattic audit`: full scan + HTML render + optional
// history insert + optional --share upload.
func newAuditCmd() *cobra.Command {
	var (
		printCleanup bool
		share        bool
	)
	c := &cobra.Command{
		Use:   "audit",
		Short: "Full audit with HTML report",
		Long: `Run a full read-only scan and emit a single-file HTML report.

The HTML report always includes a cleanup script section (SPEC §3.1) listing
only topics that satisfy every inclusion rule. The default output path is
./attic-report.html.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Minute)
			defer cancel()
			snap, err := runScan(ctx, cfg)
			if err != nil {
				return err
			}

			outPath := flags.output
			if outPath == "" {
				outPath = defaultAuditOutput
			}
			out, closer, err := openOutput(outPath)
			if err != nil {
				return err
			}
			defer closer()
			rerr := renderHTML(out, snap, cfg)
			if rerr != nil {
				return rerr
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "Wrote HTML report: %s\n", outPath)

			if printCleanup {
				// Print just the cleanup script section to stdout in addition
				// to the HTML file. The simplest correct behavior is to
				// render the full HTML to a buffer-discard pass and ALSO
				// produce a terminal-friendly cleanup listing. For v1 we
				// surface a minimal list.
				printCleanupSection(cmd.OutOrStdout(), snap)
			}

			if h := cfg.History; h != nil && h.Enabled {
				herr := insertHistory(ctx, snap, h)
				if herr != nil {
					// Non-fatal: history is optional infrastructure.
					fmt.Fprintf(cmd.ErrOrStderr(), "warning: history insert failed: %v\n", herr)
				}
			}

			extraFlags := []string{"--format", "html"}
			if share {
				extraFlags = append(extraFlags, "--share")
				url, err := uploadShare(ctx, cfg, snap)
				if err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "warning: share upload failed: %v\n", err)
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "Share URL: %s\n", url)
				}
			}
			pingTelemetryAsync(cfg, snap, extraFlags, 0)
			return nil
		},
	}
	c.Flags().BoolVar(&printCleanup, "print-cleanup", false, "print the cleanup script section to the terminal")
	c.Flags().BoolVar(&share, "share", false, "upload an anonymized summary to attic.conduktor.io and print the share URL")
	return c
}

// renderHTML writes the SPEC §3 single-file HTML report to w.
func renderHTML(w io.Writer, snap *types.Snapshot, cfg *config.Config) error {
	_ = cfg // cfg is wired for future UTM/CTA overrides per SPEC.
	return html.Render(w, snap, html.Config{})
}

// printCleanupSection prints a minimal cleanup listing to the terminal. The
// SPEC reserves the rich rendering for the HTML report; this is the CLI echo.
func printCleanupSection(w io.Writer, snap *types.Snapshot) {
	fmt.Fprintln(w, "Cleanup candidates (LIKELY_UNUSED with full evidence):")
	found := false
	for _, t := range snap.Topics {
		if t.Attic.Verdict != types.VerdictLikelyUnused {
			continue
		}
		// Apply the §3.1 inclusion rules in spirit: no MISSING_SIGNAL /
		// COMPACTED / REMOTE_STORAGE.
		blocked := false
		for _, f := range t.Flags {
			switch f {
			case types.FlagMissingSignal, types.FlagCompacted, types.FlagRemoteStorage:
				blocked = true
			default:
				// Other flags do not block cleanup inclusion.
			}
		}
		if blocked {
			continue
		}
		found = true
		fmt.Fprintf(w, "  - %s\n", t.Name)
	}
	if !found {
		fmt.Fprintln(w, "  (none)")
	}
}

// insertHistory writes the snapshot into the optional local SQLite history
// store. Failures here do not abort the audit.
func insertHistory(ctx context.Context, snap *types.Snapshot, hc *config.HistoryConfig) error {
	store, err := history.Open(ctx, hc.Path)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	_, err = store.Insert(ctx, snap, hc.RetentionDays)
	return err
}

// uploadShare builds an anonymized share summary and posts it.
func uploadShare(ctx context.Context, cfg *config.Config, snap *types.Snapshot) (string, error) {
	endpoint := telemetry.DefaultShareEndpoint
	if cfg != nil && cfg.Telemetry != nil && cfg.Telemetry.Endpoint != "" {
		// If telemetry endpoint is overridden, place share at /share for parity
		// with DefaultShareEndpoint's convention.
		endpoint = cfg.Telemetry.Endpoint + "/share"
	}
	sharer := telemetry.NewSharer(endpoint)
	sharer.Client = &http.Client{Timeout: telemetry.ShareTimeout}
	payload, err := telemetry.BuildSharePayload(version, snap.Telemetry.AnonymousRunUUID, snap)
	if err != nil {
		return "", err
	}
	resp, err := sharer.Send(ctx, payload)
	if err != nil {
		return "", err
	}
	return resp.URL, nil
}
