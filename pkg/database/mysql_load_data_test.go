package database

import (
	"errors"
	"testing"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
)

func TestFormatValueForLoadData(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		colType string
		want    string
	}{
		{"empty text is NULL", "", "TEXT", `\N`},
		{"empty numeric is NULL", "  ", "NUMERIC", `\N`},
		{"numeric comma decimal converted to dot", "1234,56", "NUMERIC", `1234.56`},
		{"numeric unparsable is NULL", "abc", "NUMERIC", `\N`},
		{"integer unparsable is NULL", "abc", "INTEGER", `\N`},
		{"integer passthrough", "42", "INTEGER", "42"},
		{"text with tab escaped", "a\tb", "TEXT", `a\tb`},
		{"text with newline escaped", "a\nb", "TEXT", `a\nb`},
		{"text with backslash escaped", `a\b`, "TEXT", `a\\b`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := formatValueForLoadData(tc.val, tc.colType)
			if got != tc.want {
				t.Errorf("formatValueForLoadData(%q, %q) = %q, want %q", tc.val, tc.colType, got, tc.want)
			}
		})
	}
}

func TestIsLocalInfileDisabledErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"error 1148", errors.New("Error 1148: the used command is not allowed with this MySQL version"), true},
		{"local_infile mentioned", errors.New("local_infile is disabled"), true},
		{"unrelated error", errors.New("Error 1062: Duplicate entry for key 'PRIMARY'"), false},
		{"connection error", errors.New("connection refused"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isLocalInfileDisabledErr(tc.err); got != tc.want {
				t.Errorf("isLocalInfileDisabledErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// While LOAD DATA is available it isn't placeholder-bound, so BatchLimit
// should hand back BatchSize as-is (not divided down by column count) —
// otherwise a wide table like estabelecimento (~30 cols) would still get
// chopped into ~2000-row batches for no reason, defeating most of the win.
func TestMySQLBatchLimitUsesLoadDataWhenAvailable(t *testing.T) {
	m := &MySQLDriver{cfg: &config.Config{BatchSize: 10000}}

	if got := m.BatchLimit(30); got != 10000 {
		t.Errorf("expected BatchLimit(30) = 10000 while LOAD DATA is available, got %d", got)
	}
}

// Once LOAD DATA has been marked unavailable (server rejected it), BatchLimit
// must fall back to a placeholder-safe size so the INSERT IGNORE fallback
// never builds an oversized multi-row statement.
func TestMySQLBatchLimitFallsBackWhenLoadDataUnavailable(t *testing.T) {
	m := &MySQLDriver{cfg: &config.Config{BatchSize: 10000}}
	m.loadDataUnavailable.Store(true)

	got := m.BatchLimit(30)
	want := placeholderBatchLimit(30, 10000, 65000)
	if got != want {
		t.Errorf("expected BatchLimit(30) = %d once LOAD DATA is unavailable, got %d", want, got)
	}
	if got >= 10000 {
		t.Errorf("fallback BatchLimit should be placeholder-bound (well under 10000 for 30 cols), got %d", got)
	}
}
