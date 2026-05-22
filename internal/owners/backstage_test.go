package owners

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sderosiaux/kafka-attic/internal/config"
)

func TestBackstage_SpecOwner(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.URL.Path != "/api/catalog/entities/by-name/component/default/orders-svc" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"spec":{"owner":"team-orders@acme.com"},"relations":[]}`))
	}))
	defer srv.Close()

	t.Setenv("BACKSTAGE_TOKEN", "abc-123")

	src, err := newBackstageSource(&config.OwnersBackstageConfig{
		URL: srv.URL,
		Auth: config.SRAuthConfig{
			Type:     "bearer",
			TokenEnv: "BACKSTAGE_TOKEN",
		},
		EntityPattern:    "component:default/{topic}-svc",
		FallbackRelation: "ownedBy",
	})
	if err != nil {
		t.Fatalf("newBackstageSource: %v", err)
	}

	owner, err := src.Lookup(context.Background(), "orders", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner == nil || owner.Value != "team-orders@acme.com" {
		t.Fatalf("expected team-orders, got %+v", owner)
	}
	if owner.Source != SourceBackstage {
		t.Errorf("source: got %q", owner.Source)
	}
	if owner.EntityRef == nil || *owner.EntityRef != "component:default/orders-svc" {
		t.Errorf("entity ref mismatch: %+v", owner.EntityRef)
	}
	if gotAuth != "Bearer abc-123" {
		t.Errorf("auth header: got %q", gotAuth)
	}
}

func TestBackstage_FallbackToRelations(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"spec":{"owner":""},
			"relations":[
				{"type":"dependsOn","targetRef":"component:default/x"},
				{"type":"ownedBy","targetRef":"group:default/data-platform"}
			]
		}`))
	}))
	defer srv.Close()

	src, err := newBackstageSource(&config.OwnersBackstageConfig{
		URL:              srv.URL,
		EntityPattern:    "component:default/{topic}",
		FallbackRelation: "ownedBy",
	})
	if err != nil {
		t.Fatalf("newBackstageSource: %v", err)
	}
	owner, err := src.Lookup(context.Background(), "orders", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner == nil || owner.Value != "group:default/data-platform" {
		t.Fatalf("expected fallback owner from ownedBy relation, got %+v", owner)
	}
}

func TestBackstage_NotFoundReturnsNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	src, err := newBackstageSource(&config.OwnersBackstageConfig{
		URL:           srv.URL,
		EntityPattern: "component:default/{topic}",
	})
	if err != nil {
		t.Fatalf("newBackstageSource: %v", err)
	}
	owner, err := src.Lookup(context.Background(), "orders", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner != nil {
		t.Fatalf("expected nil for 404, got %+v", owner)
	}
}

func TestBackstage_5xxReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	src, err := newBackstageSource(&config.OwnersBackstageConfig{
		URL:           srv.URL,
		EntityPattern: "component:default/{topic}",
	})
	if err != nil {
		t.Fatalf("newBackstageSource: %v", err)
	}
	_, err = src.Lookup(context.Background(), "orders", nil)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected 5xx error, got %v", err)
	}
}

func TestParseEntityRef(t *testing.T) {
	cases := []struct {
		ref, kind, ns, name string
		wantErr             bool
	}{
		{"component:default/orders", "component", "default", "orders", false},
		{"component:orders", "component", "default", "orders", false},
		{"orders", "", "", "", true},
		{":default/orders", "", "", "", true},
		{"component:default/", "", "", "", true},
	}
	for _, tc := range cases {
		k, ns, n, err := parseEntityRef(tc.ref)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseEntityRef(%q) err=%v wantErr=%v", tc.ref, err, tc.wantErr)
			continue
		}
		if err == nil && (k != tc.kind || ns != tc.ns || n != tc.name) {
			t.Errorf("parseEntityRef(%q) = %q/%q/%q, want %q/%q/%q",
				tc.ref, k, ns, n, tc.kind, tc.ns, tc.name)
		}
	}
}
