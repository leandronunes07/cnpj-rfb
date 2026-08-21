package database

import (
	"fmt"
	"log"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
)

// DuckDBDriver is currently a thin alias over SQLiteDriver: there is no native
// DuckDB (or Turso/libSQL) client wired in yet, only a local SQLite file. It
// exists so DB_DRIVER=duckdb/turso keep working without lying about it in the
// logs. Swap this for a real driver (e.g. marcboeker/go-duckdb or the libSQL
// client) if genuine DuckDB/Turso support is needed.
type DuckDBDriver struct {
	*SQLiteDriver
}

func NewDuckDBDriver(cfg *config.Config) *DuckDBDriver {
	log.Println("[DuckDB] AVISO: driver nativo DuckDB ainda não implementado; usando o motor SQLite local como backend.")
	return &DuckDBDriver{
		SQLiteDriver: NewSQLiteDriver(cfg),
	}
}

func (d *DuckDBDriver) Connect() error {
	if err := d.SQLiteDriver.Connect(); err != nil {
		return fmt.Errorf("duckdb (sqlite backend) initialization failed: %w", err)
	}
	log.Printf("[DuckDB] Conectado com sucesso em %s (backend SQLite)", d.cfg.DBFile)
	return nil
}
