package main

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// buildInfo is the resolved version and build metadata. The fields are populated
// from the ldflags-injected package vars when present, and from
// runtime/debug.ReadBuildInfo() otherwise (a `go install …@version` build).
type buildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
}

// resolveBuildInfo assembles the buildInfo, preferring ldflags-injected values
// and falling back to the embedded VCS stamp or module version.
func resolveBuildInfo() buildInfo {
	return buildInfo{
		Version:   resolveVersion(),
		Commit:    resolveCommit(),
		BuildDate: resolveBuildDate(),
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
	}
}

// resolveVersion returns the build version: the ldflags value when set,
// otherwise the module version (go install @vX.Y.Z) or the short VCS revision.
func resolveVersion() string {
	info, ok := debug.ReadBuildInfo()
	return resolveVersionFrom(info, ok)
}

// resolveVersionFrom is the testable core of resolveVersion: it picks the ldflags
// value, then the embedded module version or VCS revision from info.
func resolveVersionFrom(info *debug.BuildInfo, ok bool) string {
	if version != "dev" && version != "" && !isTemplateString(version) {
		return version
	}
	if ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			return info.Main.Version
		}
		if rev := vcsSetting(info, "vcs.revision"); rev != "" {
			return shortCommit(rev)
		}
	}
	return "dev"
}

// resolveCommit returns the build commit: the ldflags value when set, otherwise
// the embedded VCS revision (shortened for readability).
func resolveCommit() string {
	info, ok := debug.ReadBuildInfo()
	return resolveCommitFrom(info, ok)
}

// resolveCommitFrom is the testable core of resolveCommit.
func resolveCommitFrom(info *debug.BuildInfo, ok bool) string {
	if commit != "none" && commit != "" && !isTemplateString(commit) {
		return commit
	}
	if ok {
		if rev := vcsSetting(info, "vcs.revision"); rev != "" {
			return shortCommit(rev)
		}
	}
	return "none"
}

// resolveBuildDate returns the build date: the ldflags value when set, otherwise
// the embedded VCS commit time (or a generic marker for a go-install build).
func resolveBuildDate() string {
	info, ok := debug.ReadBuildInfo()
	return resolveBuildDateFrom(info, ok)
}

// resolveBuildDateFrom is the testable core of resolveBuildDate.
func resolveBuildDateFrom(info *debug.BuildInfo, ok bool) string {
	if buildDate != "unknown" && buildDate != "" && !isTemplateString(buildDate) {
		return buildDate
	}
	if ok {
		if ts := vcsSetting(info, "vcs.time"); ts != "" {
			if t, err := time.Parse(time.RFC3339, ts); err == nil {
				return t.UTC().Format("2006-01-02_15:04:05_UTC")
			}
			return ts
		}
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			return "go-install"
		}
	}
	return "unknown"
}

// vcsSetting returns the value of a build setting (e.g. vcs.revision), or "".
func vcsSetting(info *debug.BuildInfo, key string) string {
	for _, s := range info.Settings {
		if s.Key == key {
			return s.Value
		}
	}
	return ""
}

// shortCommit trims a full commit hash to seven characters for display.
func shortCommit(rev string) string {
	if len(rev) > 7 {
		return rev[:7]
	}
	return rev
}

// isTemplateString reports whether s still carries unsubstituted goreleaser
// template syntax (e.g. a build run outside goreleaser left "{{.Version}}").
func isTemplateString(s string) bool {
	return strings.Contains(s, "{{") && strings.Contains(s, "}}")
}

// newVersionCmd builds `flywheel version`: print the version, commit, build date,
// Go toolchain, and platform, optionally as JSON.
func newVersionCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the flywheel version and build metadata",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			info := resolveBuildInfo()
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), info)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"flywheel %s\n  commit:    %s\n  built:     %s\n  go:        %s\n  platform:  %s/%s\n",
				info.Version, info.Commit, info.BuildDate, info.GoVersion, info.OS, info.Arch)
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON instead of text")
	return cmd
}
