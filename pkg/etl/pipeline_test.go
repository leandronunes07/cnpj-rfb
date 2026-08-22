package etl

import (
	"testing"
)

func TestMatchTableSpec(t *testing.T) {
	testCases := map[string]string{
		"F.K03200$Z.D60711.CNAECSV":      "cnae",
		"K3241.K03200Y0.D60711.EMPRECSV": "empresa",
		"K3241.K03200Y0.D60711.ESTABELE": "estabelecimento",
		"K3241.K03200Y0.D60711.SOCIOCSV": "socios",
		"K3241.K03200Y0.D60711.SIMPLECS": "simples",
		"F.K03200$Z.D60711.MOTICSV":      "motivo_situacao_cadastral",
		"F.K03200$Z.D60711.MUNICCSV":     "municipio",
		"F.K03200$Z.D60711.NATJUCSV":     "natureza_juridica",
		"F.K03200$Z.D60711.PAISCSV":      "pais",
		"F.K03200$Z.D60711.QUALCSV":      "qualificacao_socio",
	}

	for filename, expectedTable := range testCases {
		spec := matchTableSpec(filename)
		if spec == nil {
			t.Errorf("expected match for %s, got nil", filename)
			continue
		}
		if spec.Name != expectedTable {
			t.Errorf("for %s expected table %s, got %s", filename, expectedTable, spec.Name)
		}
	}
}
