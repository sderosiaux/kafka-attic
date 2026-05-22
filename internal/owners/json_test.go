package owners

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sderosiaux/kafka-attic/internal/config"
)

func TestJSONSource_DefaultExtractOwner(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "topic=orders-1") {
			t.Errorf("topic placeholder not substituted: %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"owner":"team-orders@acme.com","team":"orders"}`))
	}))
	defer srv.Close()

	src, err := newJSONSource(&config.OwnersJSONConfig{
		URL: srv.URL + "/lookup?topic={topic}",
	})
	if err != nil {
		t.Fatalf("newJSONSource: %v", err)
	}
	owner, err := src.Lookup(context.Background(), "orders-1", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner == nil || owner.Value != "team-orders@acme.com" {
		t.Fatalf("expected team-orders, got %+v", owner)
	}
	if owner.Source != SourceJSON {
		t.Errorf("source: got %q", owner.Source)
	}
}

func TestJSONSource_CustomExtractAndHeaderEnv(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"data":{"team":"orders-platform"}}`))
	}))
	defer srv.Close()

	t.Setenv("OWNERS_TOKEN", "deadbeef")

	src, err := newJSONSource(&config.OwnersJSONConfig{
		URL: srv.URL + "/lookup?topic={topic}",
		Headers: map[string]string{
			"Authorization": "Bearer ${OWNERS_TOKEN}",
		},
		Extract: ".data.team",
	})
	if err != nil {
		t.Fatalf("newJSONSource: %v", err)
	}
	owner, err := src.Lookup(context.Background(), "orders-1", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner == nil || owner.Value != "orders-platform" {
		t.Fatalf("expected orders-platform, got %+v", owner)
	}
	if gotAuth != "Bearer deadbeef" {
		t.Errorf("expected header to interpolate env, got %q", gotAuth)
	}
}

func TestJSONSource_ExtractNoMatchReturnsNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"team":"x"}`))
	}))
	defer srv.Close()

	src, err := newJSONSource(&config.OwnersJSONConfig{
		URL:     srv.URL + "?topic={topic}",
		Extract: ".owner",
	})
	if err != nil {
		t.Fatalf("newJSONSource: %v", err)
	}
	owner, err := src.Lookup(context.Background(), "orders-1", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner != nil {
		t.Fatalf("expected nil, got %+v", owner)
	}
}

func TestJSONSource_404ReturnsNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()
	src, err := newJSONSource(&config.OwnersJSONConfig{URL: srv.URL + "?t={topic}"})
	if err != nil {
		t.Fatalf("newJSONSource: %v", err)
	}
	owner, err := src.Lookup(context.Background(), "orders-1", nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if owner != nil {
		t.Fatalf("expected nil for 404, got %+v", owner)
	}
}

func TestJSONSource_InvalidExtractAtParseTime(t *testing.T) {
	_, err := newJSONSource(&config.OwnersJSONConfig{
		URL:     "http://x",
		Extract: ".[invalid",
	})
	if err == nil {
		t.Fatal("expected parse error for invalid jq expression")
	}
}

func TestExpandEnv(t *testing.T) {
	t.Setenv("FOO", "bar")
	t.Setenv("BAZ", "qux")
	cases := map[string]string{
		"$FOO":          "bar",
		"${FOO}":        "bar",
		"Bearer ${FOO}": "Bearer bar",
		"$FOO-$BAZ":     "bar-qux",
		"no vars here":  "no vars here",
		"$MISSING":      "",
		"${MISSING}":    "",
		"$":             "$",
	}
	for in, want := range cases {
		got := expandEnv(in)
		if got != want {
			t.Errorf("expandEnv(%q) = %q, want %q", in, got, want)
		}
	}
}
