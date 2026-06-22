package main

import (
	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/gorm"
)

// loadAndOpen loads the config at path and opens its database, returning both
// plus the matching driver — the shared first step of every DB-backed command.
func loadAndOpen(path string) (*Config, *gorm.DB, flywheel.Driver, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, nil, nil, err
	}
	db, driver, err := openDB(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	return cfg, db, driver, nil
}

// closeDB closes the database handle behind db, ignoring a missing handle.
func closeDB(db *gorm.DB) {
	if sqlDB, err := db.DB(); err == nil {
		_ = sqlDB.Close()
	}
}

// dbLabel renders a short, secret-free description of the configured database for
// logs and command output.
func dbLabel(cfg *Config) string {
	if cfg.DB.Postgres != "" {
		return "postgres"
	}
	if path, err := sqlitePath(cfg); err == nil {
		return "sqlite:" + path
	}
	return "sqlite"
}
