package database

import (
	"fmt"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/schema"
)

type DBDriver interface {
	Connect() error
	Close() error
	InitSchema() error
	GetLatestProcessedMonth() (string, error)
	SaveProcessedMonth(month string) error
	InsertBatch(table schema.TableSpec, rows [][]string) error
	GetCNPJ(cnpj string) (map[string]interface{}, error)
	SearchCNPJ(query string, uf string, limit int) ([]map[string]interface{}, error)
	GetStats() (map[string]interface{}, error)
}

func NewDBDriver(cfg *config.Config) (DBDriver, error) {
	switch cfg.DBDriver {
	case "postgres":
		return NewPostgresDriver(cfg), nil
	case "mysql":
		return NewMySQLDriver(cfg), nil
	case "sqlite", "turso":
		return NewSQLiteDriver(cfg), nil
	case "duckdb":
		return NewDuckDBDriver(cfg), nil
	case "clickhouse":
		return NewClickHouseDriver(cfg), nil
	default:
		return nil, fmt.Errorf("driver de banco não suportado: %s", cfg.DBDriver)
	}
}

func NullIfEmpty(val string) interface{} {
	if val == "" {
		return nil
	}
	return val
}
