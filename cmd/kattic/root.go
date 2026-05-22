package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/conduktor/kafka-attic/internal/telemetry"
)

// globalFlags holds the four CLI-wide flags wired by M0. They're declared at
// package scope so every subcommand can read them via PersistentPreRun.
type globalFlags struct {
	cluster string
	config  string
	output  string
	format  string
}

var flags globalFlags

// defaultClusterConfigPath is the path used when --cluster / --config are not
// provided. SPEC §3 implies kattic.yaml in the working directory is the
// conventional default.
const defaultClusterConfigPath = "./kattic.yaml"

// newRootCmd builds the `kattic` cobra tree. Kept as a constructor so tests
// can exercise the command graph without running main().
func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "kattic",
		Short: "Find your Kafka topic graveyard",
		Long: `kattic is the CLI for the kafka-attic project.

It scans a Kafka cluster read-only and surfaces stale, empty, oversized, and
abandoned topics with a per-topic ATTIC Score. kafka-attic never produces to
Kafka and never reads record contents.`,
		Version:       fmt.Sprintf("%s (commit %s, built %s)", version, commit, date),
		SilenceUsage:  true,
		SilenceErrors: false,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Run the first-run telemetry consent prompt before any subcommand
			// touches a cluster. The prompt is a no-op on second runs and when
			// stdin is not a TTY.
			return ensureConsent(cmd)
		},
	}

	root.PersistentFlags().StringVar(&flags.cluster, "cluster", "", "path to a cluster YAML config (kattic.yaml)")
	root.PersistentFlags().StringVar(&flags.config, "config", "", "path to a global kattic config (overrides defaults)")
	root.PersistentFlags().StringVar(&flags.output, "output", "", "output file path (default: stdout)")
	root.PersistentFlags().StringVar(&flags.format, "format", "table", "output format: table | json | csv | html")

	root.AddCommand(
		newScanCmd(),
		newAuditCmd(),
		newInspectCmd(),
		newDiffCmd(),
	)

	return root
}

// resolveConfigPath returns the first non-empty of (--cluster, --config,
// default).
func resolveConfigPath() string {
	if flags.cluster != "" {
		return flags.cluster
	}
	if flags.config != "" {
		return flags.config
	}
	return defaultClusterConfigPath
}

// ensureConsent runs the telemetry first-run prompt. Failures here never
// block the command: telemetry is opt-in and best-effort.
func ensureConsent(cmd *cobra.Command) error {
	store, err := telemetry.DefaultStore()
	if err != nil {
		// No home dir — skip silently.
		return nil
	}
	interactive := isTerminal(os.Stdin)
	_, _ = telemetry.EnsurePrompted(store, telemetry.Prompter{
		In:          os.Stdin,
		Out:         cmd.ErrOrStderr(),
		Interactive: interactive,
	})
	return nil
}
