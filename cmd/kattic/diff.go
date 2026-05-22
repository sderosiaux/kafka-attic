package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/conduktor/kafka-attic/internal/history"
	"github.com/conduktor/kafka-attic/internal/types"
)

// newDiffCmd wires `kattic diff OLD.json NEW.json`: loads two snapshots from
// disk and prints the reclaim delta.
func newDiffCmd() *cobra.Command {
	var jsonOut bool
	c := &cobra.Command{
		Use:   "diff [old.json] [new.json]",
		Short: "Compare two prior JSON snapshots",
		Long:  "Compare two snapshots produced by `kattic audit` and print the reclaim delta.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			oldSnap, err := loadSnapshotFile(args[0])
			if err != nil {
				return fmt.Errorf("load %s: %w", args[0], err)
			}
			newSnap, err := loadSnapshotFile(args[1])
			if err != nil {
				return fmt.Errorf("load %s: %w", args[1], err)
			}
			report := history.Diff(oldSnap, newSnap)
			out, closer, err := openOutput(flags.output)
			if err != nil {
				return err
			}
			defer closer()
			if flags.output == "" {
				out = cmd.OutOrStdout()
			}
			if jsonOut || flags.format == "json" {
				return report.RenderJSON(out)
			}
			return report.RenderHuman(out)
		},
	}
	c.Flags().BoolVar(&jsonOut, "json", false, "emit machine-readable JSON diff")
	return c
}

// loadSnapshotFile reads a JSON snapshot from disk.
func loadSnapshotFile(path string) (*types.Snapshot, error) {
	b, err := os.ReadFile(filepath.Clean(path)) //nolint:gosec // operator-supplied snapshot path
	if err != nil {
		return nil, err
	}
	var snap types.Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	return &snap, nil
}
