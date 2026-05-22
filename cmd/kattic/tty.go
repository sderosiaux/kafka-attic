package main

import (
	"os"

	"github.com/mattn/go-isatty"
)

// isTerminal returns true when f is a TTY. Wraps go-isatty (already pulled in
// transitively via cobra) so we can keep the import in one place.
func isTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
}
