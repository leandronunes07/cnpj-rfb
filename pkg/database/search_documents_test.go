package database

import (
	"fmt"
	"testing"

	"github.com/leandronunes07/cnpj-rfb/pkg/schema"
)

func findTableSpec(t *testing.T, name string) schema.TableSpec {
	t.Helper()
	for _, tb := range schema.Tables {
		if tb.Name == name {
			return tb
		}
	}
	t.Fatalf("table spec %q not found", name)
	return schema.TableSpec{}
}

// seedSearchDocuments inserts n paired empresa+estabelecimento rows so
// GetSearchDocuments has real joined data to paginate over.
func seedSearchDocuments(t *testing.T, driver *SQLiteDriver, n int) {
	t.Helper()
	empresaTable := findTableSpec(t, "empresa")
	estabTable := findTableSpec(t, "estabelecimento")

	for i := 0; i < n; i++ {
		cnpjBasico := fmt.Sprintf("%08d", i)

		empresaRow := [][]string{{cnpjBasico, fmt.Sprintf("EMPRESA %d LTDA", i), "2062", "50", "1000,00", "5", ""}}
		if err := driver.InsertBatch(empresaTable, empresaRow); err != nil {
			t.Fatalf("failed seeding empresa row %d: %v", i, err)
		}

		// estabelecimento columns: cnpj_basico, cnpj_ordem, cnpj_dv,
		// identificador_matriz_filial, nome_fantasia, situacao_cadastral,
		// data_situacao_cadastral, motivo_situacao_cadastral,
		// nome_cidade_exterior, pais, data_inicio_atividade,
		// cnae_fiscal_principal, cnae_fiscal_secundaria, tipo_logradouro,
		// logradouro, numero, complemento, bairro, cep, uf, municipio,
		// ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax,
		// correio_eletronico, situacao_especial, data_situacao_especial
		estabRow := [][]string{{
			cnpjBasico, "0001", "90", "1", fmt.Sprintf("FANTASIA %d", i), "2", "20200101", "0",
			"", "", "20200101", "6201500", "", "", "", "", "", "", "", "SP", "7107",
			"", "", "", "", "", "", "", "", "",
		}}
		if err := driver.InsertBatch(estabTable, estabRow); err != nil {
			t.Fatalf("failed seeding estabelecimento row %d: %v", i, err)
		}
	}
}

func TestGetSearchDocumentsPaginatesWithHasMore(t *testing.T) {
	driver := newTestSQLiteDriver(t)
	seedSearchDocuments(t, driver, 5)

	page1, hasMore, err := driver.GetSearchDocuments(0, 2)
	if err != nil {
		t.Fatalf("GetSearchDocuments(0, 2) failed: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 documents on page 1, got %d", len(page1))
	}
	if !hasMore {
		t.Errorf("expected hasMore=true with 5 rows total and limit=2 at offset=0")
	}

	page3, hasMore, err := driver.GetSearchDocuments(4, 2)
	if err != nil {
		t.Fatalf("GetSearchDocuments(4, 2) failed: %v", err)
	}
	if len(page3) != 1 {
		t.Fatalf("expected 1 document on the final page (5 rows, offset 4), got %d", len(page3))
	}
	if hasMore {
		t.Errorf("expected hasMore=false on the final page, got true")
	}
}

func TestGetSearchDocumentsJoinsEmpresaAndEstabelecimento(t *testing.T) {
	driver := newTestSQLiteDriver(t)
	seedSearchDocuments(t, driver, 1)

	docs, _, err := driver.GetSearchDocuments(0, 10)
	if err != nil {
		t.Fatalf("GetSearchDocuments failed: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}

	got := docs[0]
	if got.RazaoSocial != "EMPRESA 0 LTDA" {
		t.Errorf("razao_social = %q, want %q (from the empresa join)", got.RazaoSocial, "EMPRESA 0 LTDA")
	}
	if got.NomeFantasia != "FANTASIA 0" {
		t.Errorf("nome_fantasia = %q, want %q", got.NomeFantasia, "FANTASIA 0")
	}
	if got.UF != "SP" {
		t.Errorf("uf = %q, want %q", got.UF, "SP")
	}
	if got.CNAE != 6201500 {
		t.Errorf("cnae = %d, want %d", got.CNAE, 6201500)
	}
	if got.CNPJ != "00000000000190" {
		t.Errorf("cnpj = %q, want %q (cnpj_basico+ordem+dv concatenated)", got.CNPJ, "00000000000190")
	}
}

func TestGetSearchDocumentsEmptyTable(t *testing.T) {
	driver := newTestSQLiteDriver(t)

	docs, hasMore, err := driver.GetSearchDocuments(0, 10)
	if err != nil {
		t.Fatalf("GetSearchDocuments on an empty table should not error, got: %v", err)
	}
	if len(docs) != 0 {
		t.Errorf("expected 0 documents from an empty table, got %d", len(docs))
	}
	if hasMore {
		t.Errorf("expected hasMore=false on an empty table")
	}
}
