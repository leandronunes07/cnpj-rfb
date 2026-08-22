package search

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestServer fakes just enough of the real Meilisearch REST API (exact
// paths/methods/status codes taken from the meilisearch-go client source)
// to exercise this package's request construction and response parsing for
// real, without needing a live Meilisearch instance in this environment.
func newTestServer(t *testing.T, onRequest func(r *http.Request)) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		onRequest(r)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "available"})
	})

	mux.HandleFunc("PUT /indexes/{uid}/settings/searchable-attributes", func(w http.ResponseWriter, r *http.Request) {
		onRequest(r)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"taskUid": 1, "status": "enqueued"})
	})

	mux.HandleFunc("PUT /indexes/{uid}/settings/filterable-attributes", func(w http.ResponseWriter, r *http.Request) {
		onRequest(r)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"taskUid": 2, "status": "enqueued"})
	})

	mux.HandleFunc("POST /indexes/{uid}/documents", func(w http.ResponseWriter, r *http.Request) {
		onRequest(r)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"taskUid": 3, "status": "enqueued"})
	})

	mux.HandleFunc("POST /indexes/{uid}/search", func(w http.ResponseWriter, r *http.Request) {
		onRequest(r)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"hits": []map[string]any{
				{"cnpj": "00000000000191", "razao_social": "AGENCIA TARUGA LTDA", "nome_fantasia": "TARUGA", "uf": "MG", "cnae": int64(6201500)},
			},
			"query":            "taruga",
			"processingTimeMs": 1,
		})
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestClientHealthy(t *testing.T) {
	srv := newTestServer(t, func(r *http.Request) {})
	c := New(srv.URL, "test-key", "estabelecimentos")

	if !c.Healthy() {
		t.Fatal("expected Healthy() to be true against a server answering 200 on /health")
	}
}

func TestClientHealthyFalseWhenUnreachable(t *testing.T) {
	c := New("http://127.0.0.1:1", "test-key", "estabelecimentos")
	if c.Healthy() {
		t.Fatal("expected Healthy() to be false against an unreachable address")
	}
}

func TestEnsureIndexHitsBothSettingsEndpoints(t *testing.T) {
	var hitPaths []string
	srv := newTestServer(t, func(r *http.Request) {
		hitPaths = append(hitPaths, r.Method+" "+r.URL.Path)
	})
	c := New(srv.URL, "test-key", "estabelecimentos")

	if err := c.EnsureIndex(); err != nil {
		t.Fatalf("EnsureIndex failed: %v", err)
	}

	wantPaths := []string{
		"PUT /indexes/estabelecimentos/settings/searchable-attributes",
		"PUT /indexes/estabelecimentos/settings/filterable-attributes",
	}
	for _, want := range wantPaths {
		found := false
		for _, got := range hitPaths {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected a request to %q, got requests: %v", want, hitPaths)
		}
	}
}

func TestIndexDocumentsEmptyIsNoop(t *testing.T) {
	called := false
	srv := newTestServer(t, func(r *http.Request) { called = true })
	c := New(srv.URL, "test-key", "estabelecimentos")

	if err := c.IndexDocuments(nil); err != nil {
		t.Fatalf("IndexDocuments(nil) should be a no-op, got error: %v", err)
	}
	if called {
		t.Error("IndexDocuments with no documents should not make any HTTP request")
	}
}

func TestIndexDocumentsSendsToDocumentsEndpoint(t *testing.T) {
	var hitPath string
	srv := newTestServer(t, func(r *http.Request) {
		if strings.Contains(r.URL.Path, "/documents") {
			hitPath = r.Method + " " + r.URL.Path
		}
	})
	c := New(srv.URL, "test-key", "estabelecimentos")

	docs := []Document{{CNPJ: "00000000000191", RazaoSocial: "AGENCIA TARUGA LTDA", UF: "MG"}}
	if err := c.IndexDocuments(docs); err != nil {
		t.Fatalf("IndexDocuments failed: %v", err)
	}

	want := "POST /indexes/estabelecimentos/documents"
	if hitPath != want {
		t.Errorf("expected request to %q, got %q", want, hitPath)
	}
}

func TestSearchParsesHitsIntoDocuments(t *testing.T) {
	srv := newTestServer(t, func(r *http.Request) {})
	c := New(srv.URL, "test-key", "estabelecimentos")

	docs, err := c.Search("taruga", "", 20)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	got := docs[0]
	if got.CNPJ != "00000000000191" || got.RazaoSocial != "AGENCIA TARUGA LTDA" || got.UF != "MG" || got.CNAE != 6201500 {
		t.Errorf("unexpected document decoded: %+v", got)
	}
}

func TestSearchWithUFSetsFilter(t *testing.T) {
	var body []byte
	srv := newTestServer(t, func(r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/search") {
			body, _ = io.ReadAll(r.Body)
		}
	})
	c := New(srv.URL, "test-key", "estabelecimentos")

	if _, err := c.Search("taruga", "mg", 20); err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if !strings.Contains(string(body), `"filter":"uf = \"MG\""`) {
		t.Errorf("expected request body to contain an uppercased uf filter, got: %s", body)
	}
}
