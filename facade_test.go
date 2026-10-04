package flywheel_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// facadeExempt names the exported implementation symbols the facade
// deliberately does not forward, each with the reason. Anything else exported
// from internal/core or internal/node must be reachable through the facade.
//
//nolint:gochecknoglobals // shared expectation fixture
var facadeExempt = map[string]string{}

// exportedDecls returns the exported top-level names declared in dir's non-test
// Go files: functions (not methods), types, constants, and variables.
func exportedDecls(t *testing.T, dir string) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	out := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					out[d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch sp := spec.(type) {
					case *ast.TypeSpec:
						if sp.Name.IsExported() {
							out[sp.Name.Name] = true
						}
					case *ast.ValueSpec:
						for _, n := range sp.Names {
							if n.IsExported() {
								out[n.Name] = true
							}
						}
					}
				}
			}
		}
	}
	return out
}

// TestFacadeForwardsEveryExportedSymbol is the completeness guard for the
// facade: every exported function, type, constant, and variable in the
// implementation packages has a same-named counterpart in package flywheel. A
// new API added to internal/core without a forwarder is unreachable to a
// consumer — internal/ forbids the import — and this is where that shows up,
// rather than in the consumer's build.
func TestFacadeForwardsEveryExportedSymbol(t *testing.T) {
	t.Parallel()

	facade := exportedDecls(t, ".")
	var missing []string
	for _, dir := range []string{"internal/core", "internal/node"} {
		for name := range exportedDecls(t, dir) {
			if _, exempt := facadeExempt[name]; exempt {
				continue
			}
			if !facade[name] {
				missing = append(missing, dir+"."+name)
			}
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing, "exported implementation symbols with no facade forwarder")
}
