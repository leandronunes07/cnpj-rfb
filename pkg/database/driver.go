package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/schema"
)

type DBDriver interface {
	Connect() error
	Close() error
	InitSchema() error
	// EnsureIndexes creates secondary (non-PK) indexes if they don't exist yet.
	// It is intentionally NOT part of InitSchema: building these indexes before
	// a bulk load makes every insert pay for index maintenance. The pipeline
	// calls this after loading data instead, so the first full backfill writes
	// against bare tables and indexes are built once, in bulk, at the end.
	EnsureIndexes() error
	// BatchLimit returns how many rows the pipeline should buffer per
	// InsertBatch call for a table with the given number of columns. Drivers
	// that bind rows to placeholders (Postgres/SQLite multi-row INSERT) must
	// keep this under their placeholder ceiling; drivers without that
	// constraint (MySQL via LOAD DATA, ClickHouse's row-by-row prepared
	// statement) are free to return a larger, memory-bound value instead.
	BatchLimit(numCols int) int
	GetLatestProcessedMonth() (string, error)
	SaveProcessedMonth(month string) error
	IsFileProcessed(dataMonth string, filename string) (bool, error)
	SaveProcessedFile(dataMonth string, filename string, status string) error
	InsertBatch(table schema.TableSpec, rows [][]string) error
	// UpsertBatchTracked behaves like InsertBatch for tables with a stable
	// natural key (empresa, estabelecimento, simples): a row whose primary
	// key doesn't exist yet is inserted exactly as InsertBatch would; a row
	// whose primary key already exists is compared field-by-field against
	// what's stored, and only updated (with each changed field recorded,
	// see GetChangeHistory) if something actually differs. Rows that match
	// exactly are skipped — no write at all — which is what keeps a monthly
	// reimport cheap: most companies don't change from one competência to
	// the next. Tables without tracking support fall back to plain
	// InsertBatch unchanged (see each driver for which tables it tracks).
	UpsertBatchTracked(table schema.TableSpec, rows [][]string, competencia string) error
	// GetChangeHistory returns the field-level changes UpsertBatchTracked
	// has recorded for a company (matched by its 8-digit cnpj_basico —
	// shared across all of its establishments), newest first. A driver that
	// doesn't implement tracking yet returns an empty slice, not an error.
	GetChangeHistory(cnpjBasico string) ([]ChangeLogEntry, error)
	GetCNPJ(cnpj string) (map[string]interface{}, error)
	SearchCNPJ(query string, uf string, limit int) ([]map[string]interface{}, error)
	GetStats() (map[string]interface{}, error)
	// GetSearchDocuments returns a page of denormalized, joined records for
	// bulk-exporting into an external search index (see pkg/search) — the
	// same estabelecimento+empresa join SearchCNPJ itself uses, just
	// paginated and without a query filter. hasMore reports whether another
	// call with a higher offset would return more rows.
	GetSearchDocuments(offset, limit int) (docs []SearchDocument, hasMore bool, err error)
}

// SearchDocument is the flat, denormalized record exported to an external
// search index (pkg/search.Document mirrors this shape on the other side of
// that boundary — kept as two separate types so pkg/database doesn't need
// to import pkg/search).
type SearchDocument struct {
	CNPJ         string
	RazaoSocial  string
	NomeFantasia string
	UF           string
	CNAE         int64
}

// ChangeLogEntry is one recorded field-level change, produced by
// UpsertBatchTracked and read back by GetChangeHistory.
type ChangeLogEntry struct {
	Tabela      string    `json:"tabela"`
	Campo       string    `json:"campo"`
	ValorAntigo string    `json:"valor_antigo"`
	ValorNovo   string    `json:"valor_novo"`
	Competencia string    `json:"competencia"`
	DetectadoEm time.Time `json:"detectado_em"`
}

func NewDBDriver(cfg *config.Config) (DBDriver, error) {
	switch cfg.DBDriver {
	case "postgres":
		return NewPostgresDriver(cfg), nil
	case "mysql":
		return NewMySQLDriver(cfg), nil
	case "sqlite":
		return NewSQLiteDriver(cfg), nil
	case "turso", "duckdb":
		// NOTE: no native Turso/libSQL or DuckDB client is wired in yet.
		// Both currently fall back to a local SQLite file (see duckdb.go).
		return NewDuckDBDriver(cfg), nil
	case "clickhouse":
		return NewClickHouseDriver(cfg), nil
	default:
		return nil, fmt.Errorf("driver de banco não suportado: %s", cfg.DBDriver)
	}
}

// placeholderBatchLimit computes a safe row count for drivers that bind one
// placeholder per cell in a multi-row INSERT (Postgres, SQLite, and MySQL's
// INSERT-IGNORE fallback), so a single statement never exceeds the driver's
// placeholder ceiling.
func placeholderBatchLimit(numCols int, configuredBatchSize int, maxPlaceholders int) int {
	if numCols <= 0 {
		numCols = 1
	}
	limit := maxPlaceholders / numCols
	if limit == 0 {
		limit = 1
	}
	if configuredBatchSize > 0 && limit > configuredBatchSize {
		limit = configuredBatchSize
	}
	return limit
}

// scanSearchDocuments reads rows shaped (cnpj, razao_social, nome_fantasia,
// uf, cnae_fiscal_principal) — the common column order every driver's
// GetSearchDocuments query selects — into SearchDocuments. Callers request
// limit+1 rows from the database; this trims back to limit and reports
// whether that extra row existed (hasMore), so pagination doesn't need a
// separate COUNT(*) query.
func scanSearchDocuments(rows *sql.Rows, limit int) ([]SearchDocument, bool, error) {
	var docs []SearchDocument
	for rows.Next() {
		var cnpj, razao, fantasia, uf sql.NullString
		var cnae sql.NullInt64
		if err := rows.Scan(&cnpj, &razao, &fantasia, &uf, &cnae); err != nil {
			return nil, false, err
		}
		docs = append(docs, SearchDocument{
			CNPJ:         cnpj.String,
			RazaoSocial:  razao.String,
			NomeFantasia: fantasia.String,
			UF:           uf.String,
			CNAE:         cnae.Int64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	hasMore := len(docs) > limit
	if hasMore {
		docs = docs[:limit]
	}
	return docs, hasMore, nil
}

func NullIfEmpty(val string) interface{} {
	if val == "" {
		return nil
	}
	return val
}
