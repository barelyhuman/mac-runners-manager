// Package store persists runner registrations and their JIT configs into a
// local SQLite database so the manager can recover state across restarts and
// run background cleanup jobs against orphaned GitHub registrations.
package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// Store is a thin wrapper around a SQLite connection.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at path and runs migrations.
func Open(path string) (*Store, error) {
	dsn := path + "?_journal_mode=WAL&_busy_timeout=5000"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite works best with a single writer.
	db.SetConnMaxLifetime(0)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	s := &Store{db: db}
	if err := s.runMigrations(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) runMigrations() error {
	schema := `
CREATE TABLE IF NOT EXISTS runners (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_name    TEXT UNIQUE NOT NULL,
    target_owner     TEXT NOT NULL,
    target_repo      TEXT NOT NULL,
    labels           TEXT,
    jit_config       TEXT NOT NULL,
    github_runner_id INTEGER,
    state            TEXT NOT NULL,
    guest_ip         TEXT,
    created_at       DATETIME NOT NULL,
    updated_at       DATETIME NOT NULL,
    deleted_at       DATETIME
);
CREATE INDEX IF NOT EXISTS idx_runners_state         ON runners(state);
CREATE INDEX IF NOT EXISTS idx_runners_instance_name ON runners(instance_name);
CREATE INDEX IF NOT EXISTS idx_runners_deleted_at  ON runners(deleted_at);
`
	_, err := s.db.Exec(schema)
	return err
}
