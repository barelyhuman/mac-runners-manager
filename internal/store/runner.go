package store

import (
	"database/sql"
	"fmt"
	"time"
)

// Runner is one row in the runners table.
type Runner struct {
	ID             int64
	InstanceName   string
	TargetOwner    string
	TargetRepo     string
	Labels         sql.NullString
	JITConfig      string
	GitHubRunnerID sql.NullInt64
	State          string
	GuestIP        sql.NullString
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeletedAt      sql.NullTime
}

// GuestIPString returns the guest IP or an empty string.
func (r Runner) GuestIPString() string {
	if r.GuestIP.Valid {
		return r.GuestIP.String
	}
	return ""
}

// NullString returns a populated sql.NullString if s is non-empty,
// otherwise a zero value.
func NullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// InsertRunner inserts a new runner record as provisioning begins.
func (s *Store) InsertRunner(r Runner) error {
	const q = `
INSERT INTO runners (instance_name, target_owner, target_repo, labels, jit_config, state, guest_ip, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	now := time.Now().UTC()
	_, err := s.db.Exec(q,
		r.InstanceName,
		r.TargetOwner,
		r.TargetRepo,
		r.Labels,
		r.JITConfig,
		r.State,
		r.GuestIP,
		now,
		now,
	)
	if err != nil {
		return fmt.Errorf("insert runner %s: %w", r.InstanceName, err)
	}
	return nil
}

// UpdateState updates the state and bumps updated_at.
func (s *Store) UpdateState(instanceName, state string) error {
	const q = `UPDATE runners SET state = ?, updated_at = ? WHERE instance_name = ? AND deleted_at IS NULL`
	_, err := s.db.Exec(q, state, time.Now().UTC(), instanceName)
	if err != nil {
		return fmt.Errorf("update state %s for %s: %w", state, instanceName, err)
	}
	return nil
}

// UpdateGuestIP persists the resolved guest IP.
func (s *Store) UpdateGuestIP(instanceName, ip string) error {
	const q = `UPDATE runners SET guest_ip = ?, updated_at = ? WHERE instance_name = ? AND deleted_at IS NULL`
	_, err := s.db.Exec(q, ip, time.Now().UTC(), instanceName)
	if err != nil {
		return fmt.Errorf("update guest IP for %s: %w", instanceName, err)
	}
	return nil
}

// UpdateJITConfig updates the stored JIT config after a retry.
func (s *Store) UpdateJITConfig(instanceName, jitConfig string) error {
	const q = `UPDATE runners SET jit_config = ?, updated_at = ? WHERE instance_name = ? AND deleted_at IS NULL`
	_, err := s.db.Exec(q, jitConfig, time.Now().UTC(), instanceName)
	if err != nil {
		return fmt.Errorf("update JIT config for %s: %w", instanceName, err)
	}
	return nil
}

// UpdateGitHubRunnerID caches the GitHub runner ID for fast API deletion.
func (s *Store) UpdateGitHubRunnerID(instanceName string, runnerID int64) error {
	const q = `UPDATE runners SET github_runner_id = ?, updated_at = ? WHERE instance_name = ? AND deleted_at IS NULL`
	_, err := s.db.Exec(q, runnerID, time.Now().UTC(), instanceName)
	if err != nil {
		return fmt.Errorf("update runner ID for %s: %w", instanceName, err)
	}
	return nil
}

// SoftDelete marks a runner as deleted so the row remains for audit but is
// excluded from active queries.
func (s *Store) SoftDelete(instanceName string) error {
	const q = `UPDATE runners SET state = 'draining', deleted_at = ?, updated_at = ? WHERE instance_name = ? AND deleted_at IS NULL`
	now := time.Now().UTC()
	_, err := s.db.Exec(q, now, now, instanceName)
	if err != nil {
		return fmt.Errorf("soft delete %s: %w", instanceName, err)
	}
	return nil
}

// GetRunner looks up a single runner by instance name.
func (s *Store) GetRunner(instanceName string) (*Runner, error) {
	const q = `SELECT id, instance_name, target_owner, target_repo, labels, jit_config, github_runner_id, state, guest_ip, created_at, updated_at, deleted_at FROM runners WHERE instance_name = ?`
	return scanRunner(s.db.QueryRow(q, instanceName))
}

// ListActive returns every runner that has not been soft-deleted.
func (s *Store) ListActive() ([]Runner, error) {
	const q = `SELECT id, instance_name, target_owner, target_repo, labels, jit_config, github_runner_id, state, guest_ip, created_at, updated_at, deleted_at FROM runners WHERE deleted_at IS NULL ORDER BY created_at`
	rows, err := s.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRunners(rows)
}

// HardPruneBefore permanently deletes soft-deleted rows older than cutoff.
func (s *Store) HardPruneBefore(cutoff time.Time) (int64, error) {
	const q = `DELETE FROM runners WHERE deleted_at IS NOT NULL AND deleted_at < ?`
	res, err := s.db.Exec(q, cutoff)
	if err != nil {
		return 0, fmt.Errorf("hard prune: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ListActiveByState returns active runners filtered to a specific state.
func (s *Store) ListActiveByState(state string) ([]Runner, error) {
	const q = `SELECT id, instance_name, target_owner, target_repo, labels, jit_config, github_runner_id, state, guest_ip, created_at, updated_at, deleted_at FROM runners WHERE deleted_at IS NULL AND state = ? ORDER BY created_at`
	rows, err := s.db.Query(q, state)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRunners(rows)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanRunner(s scanner) (*Runner, error) {
	var r Runner
	err := s.Scan(
		&r.ID,
		&r.InstanceName,
		&r.TargetOwner,
		&r.TargetRepo,
		&r.Labels,
		&r.JITConfig,
		&r.GitHubRunnerID,
		&r.State,
		&r.GuestIP,
		&r.CreatedAt,
		&r.UpdatedAt,
		&r.DeletedAt,
	)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func scanRunners(rows *sql.Rows) ([]Runner, error) {
	var out []Runner
	for rows.Next() {
		r, err := scanRunner(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}
