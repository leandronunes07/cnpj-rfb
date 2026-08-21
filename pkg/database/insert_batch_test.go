package database

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/schema"
)

func newTestSQLiteDriver(t *testing.T) *SQLiteDriver {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "sqlite_insert_test_*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tempDir) })

	cfg := &config.Config{
		DBDriver: "sqlite",
		DBFile:   filepath.Join(tempDir, "test_cnpj.sqlite"),
	}

	driver := NewSQLiteDriver(cfg)
	if err := driver.Connect(); err != nil {
		t.Fatalf("failed connecting to test db: %v", err)
	}
	t.Cleanup(func() { driver.Close() })

	if err := driver.InitSchema(); err != nil {
		t.Fatalf("failed init schema: %v", err)
	}

	return driver
}

func findEmpresaTable(t *testing.T) schema.TableSpec {
	t.Helper()
	for _, tb := range schema.Tables {
		if tb.Name == "empresa" {
			return tb
		}
	}
	t.Fatal("empresa table spec not found")
	return schema.TableSpec{}
}

func TestInsertBatchInsertsRows(t *testing.T) {
	driver := newTestSQLiteDriver(t)
	table := findEmpresaTable(t)

	rows := [][]string{
		{"11111111", "EMPRESA UM LTDA", "2062", "50", "1000,00", "5", ""},
		{"22222222", "EMPRESA DOIS LTDA", "2062", "50", "2000,00", "5", ""},
	}

	if err := driver.InsertBatch(table, rows); err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}

	var count int
	if err := driver.db.QueryRow("SELECT COUNT(*) FROM empresa").Scan(&count); err != nil {
		t.Fatalf("failed counting rows: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows inserted, got %d", count)
	}

	var capital float64
	if err := driver.db.QueryRow("SELECT capital_social FROM empresa WHERE cnpj_basico = '11111111'").Scan(&capital); err != nil {
		t.Fatalf("failed reading capital_social: %v", err)
	}
	if capital != 1000.0 {
		t.Errorf("expected capital_social 1000.0 (comma decimal converted), got %v", capital)
	}
}

// Reprocessing the same file (e.g. after a retry) must not duplicate rows:
// InsertBatch relies on "INSERT OR IGNORE" against the table's primary key.
func TestInsertBatchIsIdempotentOnPrimaryKey(t *testing.T) {
	driver := newTestSQLiteDriver(t)
	table := findEmpresaTable(t)

	row := [][]string{{"33333333", "EMPRESA TRES LTDA", "2062", "50", "500,00", "5", ""}}

	if err := driver.InsertBatch(table, row); err != nil {
		t.Fatalf("first InsertBatch failed: %v", err)
	}
	if err := driver.InsertBatch(table, row); err != nil {
		t.Fatalf("second InsertBatch (duplicate) failed: %v", err)
	}

	var count int
	if err := driver.db.QueryRow("SELECT COUNT(*) FROM empresa WHERE cnpj_basico = '33333333'").Scan(&count); err != nil {
		t.Fatalf("failed counting rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 row after reinserting the same primary key, got %d", count)
	}
}

func TestInsertBatchEmptyRowsNoop(t *testing.T) {
	driver := newTestSQLiteDriver(t)
	table := findEmpresaTable(t)

	if err := driver.InsertBatch(table, nil); err != nil {
		t.Fatalf("InsertBatch with empty rows should be a no-op, got error: %v", err)
	}
}
