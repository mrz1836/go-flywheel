package core

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readmeDoc is the README whose schema-setup guide these tests hold to the code.
const readmeDoc = "../../README.md"

// numberWords spells the counts the README writes out in words.
//
//nolint:gochecknoglobals // a fixed lookup table
var numberWords = []string{
	"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
	"eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen", "twenty",
}

// readmeSchemaSetup returns the README's schema-setup guide, from its summary to
// the end of its details block, with its line breaks folded into single spaces
// so a phrase reads the same wherever the text wraps.
func readmeSchemaSetup(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(readmeDoc))
	require.NoError(t, err)
	doc := string(raw)
	start := strings.Index(doc, "Schema setup — pick exactly one install path")
	require.GreaterOrEqual(t, start, 0, "the README carries the schema-setup guide")
	end := strings.Index(doc[start:], "</details>")
	require.Positive(t, end)
	return strings.Join(strings.Fields(doc[start:start+end]), " ")
}

// spelled returns n as the README writes it.
func spelled(t *testing.T, n int) string {
	t.Helper()
	require.Less(t, n, len(numberWords), "extend numberWords to spell %d", n)
	return numberWords[n]
}

// TestReadmeNamesEveryRuntimeTable pins the schema-setup guide to Models(): it
// counts the tables Models returns and names each one, so a host that excludes
// the runtime's tables from its migration tool by name excludes all of them.
func TestReadmeNamesEveryRuntimeTable(t *testing.T) {
	t.Parallel()
	guide := readmeSchemaSetup(t)

	models := Models()
	count := spelled(t, len(models))
	assert.True(t, strings.Contains(guide, "owns "+count+" tables"),
		"the guide says it owns %s tables, the count Models returns", count)
	for _, m := range models {
		named, ok := m.(interface{ TableName() string })
		require.True(t, ok, "%T names its table", m)
		assert.True(t, strings.Contains(guide, "`"+named.TableName()+"`"), "the guide names %s", named.TableName())
	}
	for _, word := range numberWords {
		if word == count {
			continue
		}
		for _, pattern := range []string{`\b` + word + ` tables\b`, `\ball ` + word + `\b`} {
			assert.False(t, regexp.MustCompile(pattern).MatchString(guide),
				"the guide gives no other table count (%q)", pattern)
		}
	}
}

// TestReadmeCountsTheRuntimeIndexes pins the guide's index sentence to IndexSet:
// the total, the number that are correctness-bearing, and each of their names.
func TestReadmeCountsTheRuntimeIndexes(t *testing.T) {
	t.Parallel()
	guide := readmeSchemaSetup(t)

	set, err := IndexSet("postgres")
	require.NoError(t, err)
	var correctness []string
	for _, idx := range set {
		if idx.Kind == IndexCorrectness {
			correctness = append(correctness, idx.Name)
		}
	}
	sentence := spelled(t, len(set)) + " indexes, and " + spelled(t, len(correctness)) + " of them are correctness-bearing"
	assert.True(t, strings.Contains(guide, sentence),
		"the guide says %q, IndexSet's count and how many of them carry a guarantee", sentence)
	for _, name := range correctness {
		assert.True(t, strings.Contains(guide, "`"+name+"`"), "the guide names the correctness-bearing %s", name)
	}
}
