package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// unopenableConfig writes a SQLite-backed flywheel.yaml whose database directory
// cannot be created (its parent is a regular file), so every DB-backed command's
// loadAndOpen step fails deterministically and offline — the seam for exercising
// each command's "open database" error branch without touching a real datastore.
func unopenableConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))
	body := "db:\n  sqlite: " + filepath.Join(blocker, "sub", "flywheel.db") + "\n" +
		"log:\n  level: error\n"
	p := filepath.Join(dir, "flywheel.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

// unopenableSQLiteConfig returns a *Config whose SQLite parent directory cannot be
// created (its parent is a regular file), so openDB fails at the MkdirAll step.
func unopenableSQLiteConfig(t *testing.T) *Config {
	t.Helper()
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))
	return &Config{DB: DBConfig{SQLite: filepath.Join(blocker, "sub", "flywheel.db")}}
}

// migratedCLIConfig writes a SQLite-backed flywheel.yaml and runs `migrate` so the
// schema exists, returning the config path ready for the queue commands.
func migratedCLIConfig(t *testing.T) string {
	t.Helper()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	_, err := runRoot(context.Background(), "--config", cfg, "migrate")
	require.NoError(t, err)
	return cfg
}
