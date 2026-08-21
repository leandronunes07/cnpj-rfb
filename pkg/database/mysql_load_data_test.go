package database

import (
	"errors"
	"testing"
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
