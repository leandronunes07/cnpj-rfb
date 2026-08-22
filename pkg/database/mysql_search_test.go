package database

import "testing"

func TestBuildBooleanFulltextQuery(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantQuery string
		wantOK    bool
	}{
		{"single word above min length", "taruga", "+taruga*", true},
		{"two words both above min length", "agencia taruga", "+agencia* +taruga*", true},
		{"word below MySQL's 3-char min token size falls back", "ab", "", false},
		{"one short word among long ones falls back entirely", "agencia ab taruga", "", false},
		{"empty query falls back", "", "", false},
		{"whitespace-only query falls back", "   ", "", false},
		{"operator chars stripped down to a too-short word falls back", "a+b", "", false}, // "a+b" -> strips "+" -> "ab" (2 runes) < 3
		{"strips operator chars but keeps valid word", "taru+ga", "+taruga*", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := buildBooleanFulltextQuery(tc.query)
			if ok != tc.wantOK {
				t.Fatalf("buildBooleanFulltextQuery(%q) ok = %v, want %v", tc.query, ok, tc.wantOK)
			}
			if ok && got != tc.wantQuery {
				t.Errorf("buildBooleanFulltextQuery(%q) = %q, want %q", tc.query, got, tc.wantQuery)
			}
		})
	}
}

// A query that fits FULLTEXT must never require characters MySQL's boolean
// mode parser treats as operators, since those come straight from
// unsanitized user input via the API's ?q= parameter.
func TestBuildBooleanFulltextQueryStripsAllOperatorChars(t *testing.T) {
	got, ok := buildBooleanFulltextQuery(`+-<>()~*"@word`)
	if !ok {
		t.Fatalf("expected ok=true after stripping operators down to a valid word, got ok=false")
	}
	for _, forbidden := range []string{"+word", "-word", "<word", ">word", "(word", ")word", "~word", `"word`, "@word"} {
		if got == forbidden {
			t.Fatalf("operator character leaked into query: %q", got)
		}
	}
	if got != "+word*" {
		t.Errorf("got %q, want %q", got, "+word*")
	}
}
