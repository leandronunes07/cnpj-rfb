package database

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
)

func TestSQLiteFileTracking(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite_test_*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test_cnpj.sqlite")
	cfg := &config.Config{
		DBDriver: "sqlite",
		DBFile:   dbFile,
	}

	driver := NewSQLiteDriver(cfg)
	if err := driver.Connect(); err != nil {
		t.Fatalf("failed connecting to test db: %v", err)
	}
	defer driver.Close()

	if err := driver.InitSchema(); err != nil {
		t.Fatalf("failed init schema: %v", err)
	}

	dataMonth := "2026-08"
	fileName := "Empresas0.zip"

	processed, err := driver.IsFileProcessed(dataMonth, fileName)
	if err != nil {
		t.Fatalf("IsFileProcessed failed: %v", err)
	}
	if processed {
		t.Errorf("expected file to not be processed initially")
	}

	if err := driver.SaveProcessedFile(dataMonth, fileName, "SUCCESS"); err != nil {
		t.Fatalf("SaveProcessedFile failed: %v", err)
	}

	processedAfter, err := driver.IsFileProcessed(dataMonth, fileName)
	if err != nil {
		t.Fatalf("IsFileProcessed failed after save: %v", err)
	}
	if !processedAfter {
		t.Errorf("expected file to be marked as processed after saving")
	}
}
