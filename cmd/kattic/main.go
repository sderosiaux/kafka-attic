// Package main is the kattic command-line entry point.
package main

import (
	"os"
)

// Build-time variables (set via -ldflags at release time). Defaults are
// safe for source builds and `go run`.
var (
	version = "0.0.0-dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	err := newRootCmd().Execute()
	if err != nil {
		// Cobra already prints the error to stderr; exit non-zero so CI catches it.
		os.Exit(1)
	}
}
