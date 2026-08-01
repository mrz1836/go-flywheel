#!/usr/bin/env bash
#
# cohesion-check.sh — a re-runnable proof of the doc/godoc/voice cohesion gates.
#
# Run from anywhere; it cd's to the repo root. Every gate is expected to be
# empty/green on a cohesive tree, so the next contributor proves cohesion the
# same way rather than by eyeballing it. Exits non-zero if any gate fails.
#
# The heavier gate — `magex format:fix && magex lint && magex vet && magex test`,
# then `test:race`, then the `-tags=integration` suite against Postgres, plus
# `go-pre-commit run gitleaks --all-files` — is documented in docs/CONVENTIONS.md
# and is the authority for CI; this script is the fast, doc-focused half.
#
# Skip the (slow) golangci-lint passes with COHESION_SKIP_LINT=1.

set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

fail=0
section() { printf '\n=== %s ===\n' "$1"; }

# ---------------------------------------------------------------------------
# 1. Link check: every relative link + #anchor in README + docs/*.md resolves.
#    There is no one-liner for this; the checker computes GitHub heading slugs
#    so in-page nav anchors are validated too.
# ---------------------------------------------------------------------------
section "link check (README.md + docs/*.md)"
perl - README.md docs/*.md <<'PERL'
use strict; use warnings;

sub canon {
    my @out;
    for my $seg (split m{/}, shift) {
        next if $seg eq '' || $seg eq '.';
        if ($seg eq '..') { pop @out if @out && $out[-1] ne '..'; }
        else { push @out, $seg; }
    }
    return join('/', @out);
}

# GitHub's heading-anchor algorithm: strip HTML, lowercase, drop everything but
# [a-z0-9 _-] (emoji/punctuation go, including their multibyte bytes), spaces to
# hyphens. Consecutive hyphens are kept, not collapsed.
sub slugify {
    my $t = shift;
    $t =~ s/<[^>]*>//g;
    $t = lc $t;
    $t =~ s/[^a-z0-9 _-]//g;
    $t =~ s/ /-/g;
    return $t;
}

my @files = @ARGV;
my %anchors;                                  # canon(path) => { slug => 1 }

for my $f (@files) {
    open my $fh, '<', $f or do { warn "cannot open $f\n"; next };
    my $key = canon($f);
    my %seen;
    while (my $line = <$fh>) {
        if ($line =~ /^\s{0,3}#{1,6}\s+(.*?)\s*#*\s*$/) {
            my $base = slugify($1);
            my $n = $seen{$base}++;
            $anchors{$key}{$n ? "$base-$n" : $base} = 1;
            $anchors{$key}{$base} = 1;
        }
        while ($line =~ /(?:name|id)\s*=\s*"([^"]+)"/g) {
            $anchors{$key}{lc $1} = 1;
        }
    }
    close $fh;
}

my $bad = 0;
for my $f (@files) {
    open my $fh, '<', $f or next;
    local $/;
    my $content = <$fh>;
    close $fh;
    (my $dir = $f) =~ s{/[^/]*$}{};
    $dir = '.' if $dir eq $f;

    my @links;
    while ($content =~ /\]\(\s*([^)\s]+)/g)      { push @links, $1; }
    while ($content =~ /href\s*=\s*"([^"]+)"/g)  { push @links, $1; }

    for my $t (@links) {
        next if $t =~ m{^(?:https?:|mailto:|tel:|data:)}i;
        next if $t =~ /^\s*$/;
        my ($path, $anchor) = split /#/, $t, 2;
        my $target = canon($f);
        if (defined $path && length $path) {
            my $rp = canon($path =~ m{^/} ? substr($path, 1) : "$dir/$path");
            unless (-e $rp) {
                printf "  BROKEN PATH    %s -> %s\n", $f, $t;
                $bad++; next;
            }
            $target = $rp;
        }
        if (defined $anchor && length $anchor && exists $anchors{$target}) {
            unless ($anchors{$target}{lc $anchor}) {
                printf "  BROKEN ANCHOR  %s -> %s\n", $f, $t;
                $bad++;
            }
        }
    }
}

if ($bad) { print "$bad broken link(s)\n"; exit 1; }
print "all links resolve\n";
exit 0;
PERL
[ $? -ne 0 ] && fail=1

# ---------------------------------------------------------------------------
# 2. Voice: the README carries no version/history/changelog register (rule 4).
# ---------------------------------------------------------------------------
section "rule-4 voice grep (README.md must be empty)"
if grep -nE 'v[0-9]+\.[0-9]|Changed in|New in|used to |previously|no longer|breaking|migration note' README.md; then
    echo "  FAIL: rule-4 hits above"; fail=1
else
    echo "clean"
fi

# ---------------------------------------------------------------------------
# 3. Hygiene: no consumer name or planning id in any committed artifact.
#    The unambiguous proper nouns and id shapes are the automated gate; the
#    internal codenames that collide with English ("reach", "control") are left
#    to human review, since a word grep on them can never be empty.
# ---------------------------------------------------------------------------
section "hygiene grep (consumer names + planning ids)"
if grep -rniE '\b(redacted|redacted|redacted|redacted|redacted)\b|\b(FR|SC|WS)-[0-9]' \
        --include='*.go' --include='*.md' --include='*.yml' . | grep -v '^\./plans/'; then
    echo "  FAIL: hygiene hits above"; fail=1
else
    echo "clean"
fi

# ---------------------------------------------------------------------------
# 4. Godoc renders per package. `go doc ./...` is INVALID ("too many periods")
#    and `go doc` has no build-tag support at all (no -tags flag, and GOFLAGS
#    -tags is not honored), so this is a per-package loop over the buildable
#    packages. The loadtest package is entirely behind //go:build loadtest, so
#    `go doc` cannot reach it under any tag — a tagged build proves its source,
#    doc comments included, is well-formed instead.
# ---------------------------------------------------------------------------
section "go doc (per package — ./... and -tags are invalid)"
for pkg in . ./config ./observers ./workers ./cmd/flywheel; do
    if go doc "$pkg" >/dev/null; then echo "  ok   go doc $pkg"; else echo "  FAIL go doc $pkg"; fail=1; fi
done
if go build -tags=loadtest ./loadtest/... >/dev/null; then echo "  ok   go build -tags=loadtest ./loadtest/... (go doc can't reach a tagged package)"; else echo "  FAIL go build -tags=loadtest ./loadtest/..."; fail=1; fi

# ---------------------------------------------------------------------------
# 5. Every example program compiles against the current API.
# ---------------------------------------------------------------------------
section "examples build"
if go build ./examples/...; then echo "ok"; else echo "  FAIL: go build ./examples/..."; fail=1; fi

# ---------------------------------------------------------------------------
# 6. Lint under every build tag (magex lint is the authority; this mirrors its
#    tag coverage). Slow; skip with COHESION_SKIP_LINT=1.
# ---------------------------------------------------------------------------
if [ "${COHESION_SKIP_LINT:-0}" = "1" ]; then
    section "golangci-lint (SKIPPED via COHESION_SKIP_LINT=1)"
elif ! command -v golangci-lint >/dev/null 2>&1; then
    section "golangci-lint (SKIPPED — not on PATH; run 'magex lint')"
else
    section "golangci-lint (default / integration / loadtest)"
    golangci-lint run ./... || fail=1
    golangci-lint run --build-tags=integration ./... || fail=1
    golangci-lint run --build-tags=loadtest ./loadtest/... || fail=1
fi

echo
if [ "$fail" -ne 0 ]; then
    echo "COHESION CHECK: FAIL"
    exit 1
fi
echo "COHESION CHECK: PASS"
