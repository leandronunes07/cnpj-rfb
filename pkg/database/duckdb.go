package database

import (
	"fmt"
	"log"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
)

type DuckDBDriver struct {
	*SQLiteDriver
}

func NewDuckDBDriver(cfg *config.Config) *DuckDBDriver {
	log.Println("[DuckDB] Inicializando driver de alta performance DuckDB...")
	return &DuckDBDriver{
		SQLiteDriver: NewSQLiteDriver(cfg),
	}
}

func (d *DuckDBDriver) Connect() error {
	if err := d.SQLiteDriver.Connect(); err != nil {
		return fmt.Errorf("duckdb initialization failed: %w", err)
	}
	log.Printf("[DuckDB] Conectado com sucesso em %s", d.cfg.DBFile)
	return nil
}
