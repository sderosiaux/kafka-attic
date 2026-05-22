package owners

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sderosiaux/kafka-attic/internal/config"
	"github.com/sderosiaux/kafka-attic/internal/types"
)

// backstageSource queries a Backstage Catalog API for owner information.
//
// SPEC §5.4: "Hit a Backstage Catalog API: GET /api/catalog/entities/by-name/
// {kind}/{namespace}/{name}. Resolve via a configurable pattern from topic
// name → entity ref. Fallback to relations.ownedBy."
//
// The configurable pattern (e.g. "component:default/{topic}-svc") yields a
// Backstage entity reference of the form "kind:namespace/name". We split it
// into the three path segments expected by the by-name endpoint.
type backstageSource struct {
	baseURL          string
	entityPattern    string
	fallbackRelation string

	client *http.Client

	auth config.SRAuthConfig // kept for late env resolution per request
}

// defaultBackstageTimeout caps each Backstage call. The audit must complete
// even when Backstage is slow; failure on owner lookup is non-fatal.
const defaultBackstageTimeout = 10 * time.Second

func newBackstageSource(cfg *config.OwnersBackstageConfig) (Source, error) {
	if cfg == nil {
		return nil, errors.New("backstage config is nil")
	}
	base := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	if base == "" {
		return nil, errors.New("backstage url is empty")
	}
	pattern := strings.TrimSpace(cfg.EntityPattern)
	if pattern == "" {
		// Sensible default mirroring Appendix B.
		pattern = "component:default/{topic}"
	}
	fallback := strings.TrimSpace(cfg.FallbackRelation)
	if fallback == "" {
		fallback = "ownedBy"
	}

	return &backstageSource{
		baseURL:          base,
		entityPattern:    pattern,
		fallbackRelation: fallback,
		client:           &http.Client{Timeout: defaultBackstageTimeout},
		auth:             cfg.Auth,
	}, nil
}

func (s *backstageSource) Name() string { return SourceBackstage }

// backstageEntity is the minimal Backstage entity shape we consume. We
// intentionally avoid pulling the full schema and instead deserialize only
// the fields we use.
type backstageEntity struct {
	Spec struct {
		Owner string `json:"owner"`
	} `json:"spec"`
	Relations []struct {
		Type      string `json:"type"`
		TargetRef string `json:"targetRef"`
		Target    struct {
			Kind      string `json:"kind"`
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
		} `json:"target"`
	} `json:"relations"`
}

func (s *backstageSource) Lookup(ctx context.Context, topic string, _ map[string]string) (*types.OwnerInfo, error) {
	ref := strings.ReplaceAll(s.entityPattern, "{topic}", topic)
	kind, namespace, name, err := parseEntityRef(ref)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf(
		"%s/api/catalog/entities/by-name/%s/%s/%s",
		s.baseURL,
		url.PathEscape(kind),
		url.PathEscape(namespace),
		url.PathEscape(name),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	s.applyAuth(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// Topic has no matching catalog entry; not an error, just no mapping.
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, nil //nolint:nilnil // documented contract: nil owner means "not found"
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("backstage GET %s: %s", endpoint, resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	var entity backstageEntity
	uerr := json.Unmarshal(body, &entity)
	if uerr != nil {
		return nil, fmt.Errorf("backstage decode: %w", uerr)
	}

	if owner := strings.TrimSpace(entity.Spec.Owner); owner != "" {
		entityRef := ref
		return &types.OwnerInfo{
			Value:     owner,
			Source:    SourceBackstage,
			EntityRef: &entityRef,
		}, nil
	}

	// Fallback to relations[type==fallbackRelation].
	for _, rel := range entity.Relations {
		if rel.Type != s.fallbackRelation {
			continue
		}
		if v := strings.TrimSpace(rel.TargetRef); v != "" {
			entityRef := ref
			return &types.OwnerInfo{
				Value:     v,
				Source:    SourceBackstage,
				EntityRef: &entityRef,
			}, nil
		}
		// Older Backstage versions used a structured target instead of
		// targetRef. Reconstruct the canonical form when present.
		if rel.Target.Kind != "" && rel.Target.Name != "" {
			ns := rel.Target.Namespace
			if ns == "" {
				ns = "default"
			}
			v := fmt.Sprintf("%s:%s/%s", rel.Target.Kind, ns, rel.Target.Name)
			entityRef := ref
			return &types.OwnerInfo{
				Value:     v,
				Source:    SourceBackstage,
				EntityRef: &entityRef,
			}, nil
		}
	}

	return nil, nil //nolint:nilnil // documented contract: nil owner means no mapping
}

// applyAuth attaches the auth header configured in cfg.owners.backstage.auth.
//
// Env vars are resolved at request time (consistent with ResolveEnv usage
// elsewhere) so a wrapper script can mutate the environment after Load.
func (s *backstageSource) applyAuth(req *http.Request) {
	switch s.auth.Type {
	case "bearer":
		if tok := config.ResolveEnv(s.auth.TokenEnv); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	case "basic":
		user := config.ResolveEnv(s.auth.UsernameEnv)
		pass := config.ResolveEnv(s.auth.PasswordEnv)
		if user != "" || pass != "" {
			req.SetBasicAuth(user, pass)
		}
	}
}

// parseEntityRef splits "kind:namespace/name" into its three parts.
// "kind:name" (no namespace) is accepted and treated as namespace="default",
// matching Backstage's own normalisation rules.
func parseEntityRef(ref string) (kind, namespace, name string, err error) {
	colon := strings.Index(ref, ":")
	if colon <= 0 || colon == len(ref)-1 {
		return "", "", "", fmt.Errorf("backstage entity ref %q: expected kind:namespace/name", ref)
	}
	kind = ref[:colon]
	rest := ref[colon+1:]

	if before, after, ok := strings.Cut(rest, "/"); ok {
		namespace = before
		name = after
	} else {
		namespace = "default"
		name = rest
	}

	if kind == "" || namespace == "" || name == "" {
		return "", "", "", fmt.Errorf("backstage entity ref %q: empty segment", ref)
	}
	return kind, namespace, name, nil
}
