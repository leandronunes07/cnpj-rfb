// Package search wraps Meilisearch for GET /api/v1/busca's name search.
//
// The SQL-based search (LIKE/pg_trgm/FULLTEXT/ngrambf_v1, see
// pkg/database) works and is what the app falls back to whenever
// Meilisearch isn't configured or isn't reachable — this package is a
// genuine performance/quality upgrade on top of that, not a replacement
// the app depends on. Meilisearch gives real relevance ranking and typo
// tolerance that none of the SQL-side accelerations attempt.
package search

import (
	"fmt"
	"strings"

	"github.com/meilisearch/meilisearch-go"
)

// Document is the flat, denormalized record indexed for name search — the
// same shape SQL-based search already returns (see
// pkg/database.SearchDocument), so callers don't care which path served a
// given request.
type Document struct {
	CNPJ         string `json:"cnpj"`
	RazaoSocial  string `json:"razao_social"`
	NomeFantasia string `json:"nome_fantasia"`
	UF           string `json:"uf"`
	CNAE         int64  `json:"cnae"`
}

const primaryKeyField = "cnpj"

type Client struct {
	ms    meilisearch.ServiceManager
	index string
}

func New(host, apiKey, indexName string) *Client {
	return &Client{
		ms:    meilisearch.New(host, meilisearch.WithAPIKey(apiKey)),
		index: indexName,
	}
}

// Healthy reports whether Meilisearch is reachable right now.
func (c *Client) Healthy() bool {
	return c.ms.IsHealthy()
}

// EnsureIndex configures which fields are searchable/filterable. Safe to
// call repeatedly (e.g. once per pipeline run): Meilisearch no-ops when the
// settings already match, and only rebuilds the index when they change.
func (c *Client) EnsureIndex() error {
	idx := c.ms.Index(c.index)

	if _, err := idx.UpdateSearchableAttributes(&[]string{"razao_social", "nome_fantasia"}); err != nil {
		return fmt.Errorf("failed setting searchable attributes: %w", err)
	}

	filterable := []interface{}{"uf", "cnae"}
	if _, err := idx.UpdateFilterableAttributes(&filterable); err != nil {
		return fmt.Errorf("failed setting filterable attributes: %w", err)
	}

	return nil
}

// IndexDocuments upserts a batch of documents (by cnpj, the primary key).
// Meilisearch processes additions asynchronously — this returns once the
// task is enqueued, not once it's applied, which is the appropriate
// trade-off for a search index kept eventually consistent with the primary
// database rather than a system that needs read-your-writes.
func (c *Client) IndexDocuments(docs []Document) error {
	if len(docs) == 0 {
		return nil
	}
	pk := primaryKeyField
	_, err := c.ms.Index(c.index).AddDocumentsInBatches(docs, 1000, &meilisearch.DocumentOptions{PrimaryKey: &pk})
	if err != nil {
		return fmt.Errorf("failed indexing documents: %w", err)
	}
	return nil
}

// Search queries the index. uf, if non-empty, filters results the same way
// SearchCNPJ's SQL-based UF filter does.
func (c *Client) Search(query string, uf string, limit int) ([]Document, error) {
	req := &meilisearch.SearchRequest{Limit: int64(limit)}
	if uf != "" {
		req.Filter = fmt.Sprintf("uf = %q", strings.ToUpper(uf))
	}

	res, err := c.ms.Index(c.index).Search(query, req)
	if err != nil {
		return nil, fmt.Errorf("meilisearch query failed: %w", err)
	}

	docs := make([]Document, 0, len(res.Hits))
	for _, hit := range res.Hits {
		var d Document
		if err := hit.DecodeInto(&d); err != nil {
			continue
		}
		docs = append(docs, d)
	}
	return docs, nil
}
