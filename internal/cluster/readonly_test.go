package cluster

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestNoProducerCallsInNonTestSources greps the entire non-test source tree
// for any reference to franz-go producer entry points. If kafka-attic ever
// imports a producer code path, this test catches it before CI.
//
// Allowed: appearances inside *_test.go (the test corpus may reference
// patterns to scan for them) and inside string literals in this very file.
func TestNoProducerCallsInNonTestSources(t *testing.T) {
	repoRoot := findRepoRoot(t)

	// Patterns that indicate a producer was instantiated or invoked.
	// We deliberately do not list `kgo.NewClient` itself — the read-only
	// consumer factory uses it. We look for symbols that ONLY exist on the
	// producer side of the franz-go API.
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`\bProduceSync\b`),
		regexp.MustCompile(`\bProduce\(`),
		regexp.MustCompile(`\bkgo\.NewProducer\b`),
		regexp.MustCompile(`\bkgo\.TransactionalID\b`),
		regexp.MustCompile(`\bkgo\.ProducerLinger\b`),
		regexp.MustCompile(`\bkgo\.ProducerBatch`),
		regexp.MustCompile(`\bkgo\.RequiredAcks\b`),
		regexp.MustCompile(`\bkgo\.DisableIdempotentWrite\b`),
	}

	var offenders []string
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			base := d.Name()
			if base == ".git" || base == "vendor" || base == "node_modules" || base == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for _, re := range patterns {
			if re.Match(data) {
				offenders = append(offenders, path+": "+re.String())
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}
	if len(offenders) > 0 {
		t.Fatalf("producer API references found in non-test sources (kafka-attic must remain read-only):\n  %s",
			strings.Join(offenders, "\n  "))
	}
}

// findRepoRoot ascends from the test working directory until it finds go.mod
// and returns that directory. Falls back to "." if not found, which still
// scans the package tree of any caller running `go test ./...` at the root.
func findRepoRoot(t *testing.T) string {
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
	return cwd
}

func TestErrProduceForbiddenIsStable(t *testing.T) {
	if ErrProduceForbidden == nil {
		t.Fatal("ErrProduceForbidden must not be nil")
	}
	if !strings.Contains(ErrProduceForbidden.Error(), "read-only") {
		t.Fatalf("ErrProduceForbidden message should mention read-only, got %q", ErrProduceForbidden.Error())
	}
}
