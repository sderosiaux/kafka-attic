package owners

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/conduktor/kafka-attic/internal/config"
	"github.com/conduktor/kafka-attic/internal/types"
	"github.com/itchyny/gojq"
)

// defaultJSONTimeout caps each owner-JSON request. Like Backstage, owner
// lookup is best-effort and must not block the overall audit.
const defaultJSONTimeout = 10 * time.Second

// envInterpolationRE matches ${VAR_NAME} and $VAR_NAME (alnum + underscore).
// We resolve both forms because Appendix B uses ${OWNERS_TOKEN} in a header.
var envInterpolationRE = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)`)

// jsonSource is the generic HTTP GET → JSON → jq-extract owner source.
type jsonSource struct {
	urlTemplate string
	headers     map[string]string
	extract     *gojq.Query
	rawExtract  string
	client      *http.Client
}

func newJSONSource(cfg *config.OwnersJSONConfig) (Source, error) {
	if cfg == nil {
		return nil, fmt.Errorf("json config is nil")
	}
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, fmt.Errorf("json url is empty")
	}

	extractExpr := strings.TrimSpace(cfg.Extract)
	if extractExpr == "" {
		// Appendix B uses ".team"; ".owner" is the other natural default. We
		// keep extraction explicit when unset by defaulting to ".owner" so
		// users don't need to repeat the obvious case.
		extractExpr = ".owner"
	}

	q, err := gojq.Parse(extractExpr)
	if err != nil {
		return nil, fmt.Errorf("parse extract %q: %w", extractExpr, err)
	}

	// Copy headers so we own the map; do not pre-resolve env vars here —
	// resolution happens at request time, consistent with ResolveEnv usage
	// in the cluster package.
	headers := make(map[string]string, len(cfg.Headers))
	for k, v := range cfg.Headers {
		headers[k] = v
	}

	return &jsonSource{
		urlTemplate: cfg.URL,
		headers:     headers,
		extract:     q,
		rawExtract:  extractExpr,
		client:      &http.Client{Timeout: defaultJSONTimeout},
	}, nil
}

func (s *jsonSource) Name() string { return SourceJSON }

func (s *jsonSource) Lookup(ctx context.Context, topic string, _ map[string]string) (*types.OwnerInfo, error) {
	endpoint := strings.ReplaceAll(s.urlTemplate, "{topic}", topic)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	for k, v := range s.headers {
		req.Header.Set(k, expandEnv(v))
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("json owner GET %s: %s", endpoint, resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("decode owner json: %w", err)
	}

	owner, err := runExtract(s.extract, doc)
	if err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, nil
	}
	return &types.OwnerInfo{
		Value:  owner,
		Source: SourceJSON,
	}, nil
}

// runExtract runs the jq query against the decoded JSON and returns the
// first non-empty string value. Numbers and bools are coerced via fmt.
func runExtract(q *gojq.Query, doc any) (string, error) {
	iter := q.Run(doc)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, isErr := v.(error); isErr {
			return "", fmt.Errorf("jq: %w", err)
		}
		switch t := v.(type) {
		case nil:
			continue
		case string:
			if s := strings.TrimSpace(t); s != "" {
				return s, nil
			}
		case float64:
			return strings.TrimSpace(fmt.Sprintf("%v", t)), nil
		case bool:
			return fmt.Sprintf("%v", t), nil
		default:
			// Object/array: serialize to JSON so the caller still has a
			// usable string. This is a best-effort fallback; typical usage
			// targets a scalar.
			b, err := json.Marshal(t)
			if err != nil {
				continue
			}
			if s := strings.TrimSpace(string(b)); s != "" && s != "null" {
				return s, nil
			}
		}
	}
	return "", nil
}

// expandEnv resolves both ${VAR} and $VAR within a header value, using the
// process environment. Unset vars expand to "" (same semantics as os.Expand).
func expandEnv(s string) string {
	return envInterpolationRE.ReplaceAllStringFunc(s, func(match string) string {
		// Match indices: \${NAME} captures group 1; $NAME captures group 2.
		// Re-running the regex to extract the named portion is simpler than
		// threading the submatches through here.
		groups := envInterpolationRE.FindStringSubmatch(match)
		var name string
		if len(groups) >= 2 && groups[1] != "" {
			name = groups[1]
		} else if len(groups) >= 3 {
			name = groups[2]
		}
		if name == "" {
			return match
		}
		return os.Getenv(name)
	})
}
