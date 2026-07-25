//go:build loadtest

package loadtest

import (
	"strings"
	"testing"
)

// TestNewSchemaNameIsSafeByConstruction is the assertion that matters for this
// file: the schema name is concatenated into DDL, because PostgreSQL has no bind
// parameter for an identifier. Rather than escaping after the fact, the name is
// built only from base-36 renderings of integers — so the alphabet is [0-9a-z]
// by construction and there is no path by which caller input reaches it.
//
// The length bound is the second half. PostgreSQL silently truncates an
// identifier past 63 bytes, which would quietly collapse two runs into one
// schema and make a run's results depend on another run's leftovers.
func TestNewSchemaNameIsSafeByConstruction(t *testing.T) {
	t.Parallel()

	seen := make(map[string]bool)
	for range 1000 {
		name := newSchemaName()
		if !validSchemaName(name) {
			t.Fatalf("newSchemaName produced an unsafe name: %q", name)
		}
		if len(name) > maxSchemaNameLen {
			t.Fatalf("newSchemaName produced a %d-byte name, over the %d limit: %q",
				len(name), maxSchemaNameLen, name)
		}
		if !strings.HasPrefix(name, "lt_") {
			t.Fatalf("newSchemaName must be recognizable as this harness's: %q", name)
		}
		if seen[name] {
			t.Fatalf("newSchemaName repeated %q: two runs would share a schema", name)
		}
		seen[name] = true
	}
}

// TestValidSchemaNameRejectsInjection is the guard's own test. createSchema and
// dropSchema both call it before concatenating, so a hole here is a hole in the
// only thing standing between a name and the DDL.
func TestValidSchemaNameRejectsInjection(t *testing.T) {
	t.Parallel()

	bad := []string{
		"",
		"lt_a; DROP TABLE jobs",
		"lt_a--",
		`lt_a"`,
		"lt_a'",
		"lt a",
		"lt_a)",
		"public.jobs",
		"LT_A", // an uppercase name would need quoting to resolve
		"1lt",  // must not start with a digit
		"_lt",  // must start with a letter
		strings.Repeat("a", maxSchemaNameLen+1),
	}
	for _, name := range bad {
		if validSchemaName(name) {
			t.Errorf("validSchemaName(%q) = true, want false", name)
		}
	}

	good := []string{"lt_1", "lt_abc_2z", "a", strings.Repeat("a", maxSchemaNameLen)}
	for _, name := range good {
		if !validSchemaName(name) {
			t.Errorf("validSchemaName(%q) = false, want true", name)
		}
	}
}

// TestWithSearchPath proves the parameter is appended with the right separator
// for both DSN shapes, since a run whose search_path did not take would migrate
// into the public schema and collide with every sibling run.
func TestWithSearchPath(t *testing.T) {
	t.Parallel()

	cases := []struct{ dsn, want string }{
		{"postgres://h/db", "postgres://h/db?search_path=lt_1"},
		{"postgres://h/db?sslmode=disable", "postgres://h/db?sslmode=disable&search_path=lt_1"},
	}
	for _, tc := range cases {
		if got := withSearchPath(tc.dsn, "lt_1"); got != tc.want {
			t.Errorf("withSearchPath(%q) = %q, want %q", tc.dsn, got, tc.want)
		}
	}
}
