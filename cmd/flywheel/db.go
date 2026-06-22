package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// openDB opens the database the config selects and returns it with the matching
// flywheel.Driver. A non-empty Postgres DSN wins; otherwise a hardened, WAL-mode
// SQLite file is opened (single writer, busy-timeout, immediate transactions).
func openDB(cfg *Config) (*gorm.DB, flywheel.Driver, error) {
	gcfg := &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)}

	if cfg.DB.Postgres != "" {
		gdb, err := gorm.Open(postgres.Open(cfg.DB.Postgres), gcfg)
		if err != nil {
			return nil, nil, fmt.Errorf("open postgres: %w", err)
		}
		return gdb, flywheel.NewPostgresDriver(gdb), nil
	}

	path, err := sqlitePath(cfg)
	if err != nil {
		return nil, nil, err
	}
	if mkErr := os.MkdirAll(filepath.Dir(path), 0o750); mkErr != nil {
		return nil, nil, fmt.Errorf("create sqlite dir: %w", mkErr)
	}
	gdb, err := gorm.Open(sqlite.Open(sqliteDSN(path)), gcfg)
	if err != nil {
		return nil, nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, nil, fmt.Errorf("sqlite handle: %w", err)
	}
	// SQLite is a single writer; one connection serializes writes cleanly and
	// pairs with the enforced Concurrency 1.
	sqlDB.SetMaxOpenConns(1)
	return gdb, flywheel.NewSQLiteDriver(gdb), nil
}

// sqlitePath resolves the configured SQLite file path, expanding a leading ~ and
// defaulting to a per-user state file.
func sqlitePath(cfg *Config) (string, error) {
	path := cfg.DB.SQLite
	if path == "" {
		dir, err := os.UserConfigDir()
		if err != nil {
			return "", fmt.Errorf("resolve user config dir: %w", err)
		}
		return filepath.Join(dir, "flywheel", "flywheel.db"), nil
	}
	return expandHome(path)
}

// sqliteDSN builds the hardened DSN: WAL journaling for concurrent reads, a busy
// timeout to absorb the brief claim lock, immediate transactions for the
// serialized claim, and foreign keys on.
func sqliteDSN(path string) string {
	return "file:" + path + "?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate&_synchronous=NORMAL&_foreign_keys=on"
}

// expandHome expands a leading ~/ in a path to the user's home directory.
func expandHome(path string) (string, error) {
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home dir: %w", err)
		}
		if path == "~" {
			return home, nil
		}
		return filepath.Join(home, path[2:]), nil
	}
	return path, nil
}

// isSQLite reports whether the config targets SQLite (no Postgres DSN).
func isSQLite(cfg *Config) bool { return cfg.DB.Postgres == "" }
