//go:build loadtest

package loadtest

import (
	"fmt"
	"strconv"
	"strings"
)

// ruleWidth is the width of the artifact's horizontal rules. It is the width the
// summary table needs, so the rules frame the table rather than crossing it.
const ruleWidth = 118

// Text renders the report as the committed artifact.
//
// The format is text rather than JSON because the payload is a query plan: a
// plan is read line by line against its indentation, and a reader diffing
// before against after needs to see it the way psql printed it. The summary
// table above the plans is the reading aid; it never replaces them, and every
// number in it is parsed from the text below it.
func (r ExplainReport) Text() string {
	var b strings.Builder

	b.WriteString("go-flywheel — claim predicate characterization\n")
	b.WriteString(strings.Repeat("=", ruleWidth) + "\n\n")

	r.writeHeader(&b)
	r.writeLegend(&b)
	r.writeSummary(&b)
	r.writeNotes(&b)
	r.writeStatements(&b)
	r.writePlans(&b)

	return trimLineEnds(b.String())
}

// trimLineEnds strips trailing whitespace from every line.
//
// It is applied once, here, rather than pushed back into each writer. The
// summary table pads its columns to align them, and padding the last column of
// a row leaves trailing spaces on it — invisible in review, and rejected by the
// repository's whitespace check. Several other lines have the same hazard the
// moment a field they interpolate is empty. Stating the property once, at the
// only place the artifact is assembled, is what makes it hold for writers that
// do not exist yet.
func trimLineEnds(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}
	return strings.Join(lines, "\n")
}

// writeHeader states what was measured and against what.
func (r ExplainReport) writeHeader(b *strings.Builder) {
	field := func(name, value string) {
		fmt.Fprintf(b, "%-14s %s\n", name+":", value)
	}
	field("target", r.Target)
	field("server", r.Server)
	field("schema", r.Schema)
	field("jobs seeded", fmt.Sprintf("%s, all claimable", withThousands(int64(r.Jobs))))
	field("queues", strings.Join(r.Queues, ", "))
	field("classes", strings.Join(quoteEmpty(r.Classes), ", "))
	field("claim limit", strconv.Itoa(r.Limit))
	field("seed", strconv.FormatInt(r.Seed, 10))
	field("jobs relation", humanBytes(r.TableBytes)+" (table plus indexes, at measurement time)")
	b.WriteString("\n")
}

// writeLegend names the conditions, the predicate spellings, and the index
// variants, so a cell key like A/P1/V3 is readable without the plan below it.
func (r ExplainReport) writeLegend(b *strings.Builder) {
	b.WriteString("conditions\n")
	for _, c := range r.Conditions {
		fmt.Fprintf(b, "  %-4s %s\n", c.Name, c.Desc)
	}
	b.WriteString("\npredicate spellings\n")
	fmt.Fprintf(b, "  %-4s %s\n", predicateEmitted, "as the driver emits it: (executor_class = ? OR executor_class = '')")
	fmt.Fprintf(b, "  %-4s %s\n", predicateInList, "executor_class IN (?, '') — the spelling the SQLite driver already writes")
	b.WriteString("\nindex variants (every one installed under the name " + claimIndexName + ")\n")
	for _, v := range r.Variants {
		fmt.Fprintf(b, "  %-4s %s\n", v.Name, v.Desc)
		if v.DDL != "" {
			fmt.Fprintf(b, "       %s\n", v.DDL)
		}
	}
	b.WriteString("\n")
}

// summaryColumns is the summary table's layout: header text and column width.
//
//nolint:gochecknoglobals // a fixed table layout, not state
var summaryColumns = []struct {
	head  string
	width int
}{
	{"cell", 10},
	{"scan node", 40},
	{"scan rows", 10},
	{"sort", 5},
	{"removed", 10},
	{"buf hit", 9},
	{"buf read", 9},
	{"exec ms", 9},
}

// writeSummary renders the matrix as one table.
func (r ExplainReport) writeSummary(b *strings.Builder) {
	b.WriteString("summary\n")
	b.WriteString(strings.Repeat("-", ruleWidth) + "\n")
	for _, col := range summaryColumns {
		fmt.Fprintf(b, "%-*s", col.width, col.head)
	}
	b.WriteString("\n" + strings.Repeat("-", ruleWidth) + "\n")

	var prev string
	for _, cell := range r.Cells {
		// A blank line between conditions, so the four shapes read as four blocks.
		if prev != "" && cell.Condition != prev {
			b.WriteString("\n")
		}
		prev = cell.Condition

		s := cell.Summary
		sorted := "no"
		if s.Sorted {
			sorted = "YES"
		}
		values := []string{
			cell.Key(), truncate(s.Scan, summaryColumns[1].width-1), withThousands(s.ScanRows), sorted,
			withThousands(s.RowsRemoved), withThousands(s.SharedHit), withThousands(s.SharedRead),
			strconv.FormatFloat(s.ExecutionMS, 'f', 3, 64),
		}
		for i, v := range values {
			fmt.Fprintf(b, "%-*s", summaryColumns[i].width, v)
		}
		b.WriteString("\n")
		// The sort method carries the spill: "external merge  Disk: 3904kB" is the
		// finding, and a column wide enough to hold it would push the table past a
		// terminal, so it hangs under its row instead.
		if s.SortMethod != "" {
			fmt.Fprintf(b, "%-*ssort method: %s\n", summaryColumns[0].width, "", s.SortMethod)
		}
	}
	b.WriteString(strings.Repeat("-", ruleWidth) + "\n\n")
}

// writeNotes prints the caveats that qualify the numbers above them.
func (r ExplainReport) writeNotes(b *strings.Builder) {
	if len(r.Notes) == 0 {
		return
	}
	b.WriteString("notes\n")
	for _, n := range r.Notes {
		b.WriteString(wrapBullet(n, ruleWidth) + "\n")
	}
	b.WriteString("\n")
}

// writeStatements prints each captured claim once.
//
// Once, not once per cell: every variant explains the same bytes, and repeating
// a 20-line statement under every variant would bury the plans that differ
// between them under the text that does not.
func (r ExplainReport) writeStatements(b *strings.Builder) {
	b.WriteString("statements, as captured from postgresDriver.Dequeue\n")
	b.WriteString(strings.Repeat("=", ruleWidth) + "\n")
	for _, stmt := range r.Statements {
		fmt.Fprintf(b, "\n--- %s/%s ---\n%s\n", stmt.Condition, stmt.Predicate, strings.TrimSpace(stmt.SQL))
	}
	b.WriteString("\n")
}

// writePlans prints every cell's plan in full.
func (r ExplainReport) writePlans(b *strings.Builder) {
	b.WriteString("\nplans\n")
	b.WriteString(strings.Repeat("=", ruleWidth) + "\n")
	for _, cell := range r.Cells {
		fmt.Fprintf(b, "\n--- %s ---\n", cell.Key())
		for _, line := range cell.Plan {
			b.WriteString(line + "\n")
		}
	}
}

// truncate shortens s to width, marking that it did so. A silently clipped node
// description reads as a different node.
func truncate(s string, width int) string {
	if len(s) <= width {
		return s
	}
	if width <= 1 {
		return s[:max(width, 0)]
	}
	return s[:width-1] + "…"
}

// withThousands groups an integer for reading. Six- and seven-figure row counts
// are the whole point of these tables, and 1000000 and 100000 are not
// distinguishable at a glance.
func withThousands(n int64) string {
	s := strconv.FormatInt(n, 10)
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign, s = "-", s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return sign + string(out)
}

// humanBytes renders a byte count at the largest unit that keeps it above one.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	div, exp := int64(unit), 0
	for n/div >= unit && exp < 3 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGT"[exp])
}

// wrapBullet renders one note as a hanging-indent bullet at the given width.
func wrapBullet(text string, width int) string {
	const indent = "    "
	var b strings.Builder
	b.WriteString("  - ")

	line := 4
	for i, word := range strings.Fields(text) {
		if i > 0 && line+1+len(word) > width {
			b.WriteString("\n" + indent)
			line = len(indent)
		} else if i > 0 {
			b.WriteString(" ")
			line++
		}
		b.WriteString(word)
		line += len(word)
	}
	return b.String()
}
