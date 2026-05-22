package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/conduktor/kafka-attic/internal/types"
)

// newInspectCmd wires `kattic inspect --topic NAME`: emits a rich JSON
// per-signal breakdown for a single topic to stdout.
func newInspectCmd() *cobra.Command {
	var topic string
	c := &cobra.Command{
		Use:   "inspect",
		Short: "Single-topic deep dive",
		Long:  "Print every signal, evidence level, and the score breakdown for a single topic as JSON.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if topic == "" {
				return fmt.Errorf("--topic is required")
			}
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Minute)
			defer cancel()
			snap, err := runScan(ctx, cfg)
			if err != nil {
				return err
			}
			var found *types.Topic
			for i := range snap.Topics {
				if snap.Topics[i].Name == topic {
					found = &snap.Topics[i]
					break
				}
			}
			if found == nil {
				return fmt.Errorf("topic %q not found in scan (scanned %d topics)", topic, len(snap.Topics))
			}
			out, closer, err := openOutput(flags.output)
			if err != nil {
				return err
			}
			defer closer()
			if flags.output == "" {
				out = cmd.OutOrStdout()
			}

			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			enc.SetEscapeHTML(false)
			return enc.Encode(found)
		},
	}
	c.Flags().StringVar(&topic, "topic", "", "topic name to inspect")
	return c
}
