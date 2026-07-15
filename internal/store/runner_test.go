package store

import (
	"testing"
	"time"
)

func TestRunnerCRUD(t *testing.T) {
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open memory db: %v", err)
	}
	defer s.Close()

	r := Runner{
		InstanceName: "mac-runners-manager-test-123",
		TargetOwner:  "barelyhuman",
		TargetRepo:   "mac-runners-manager",
		JITConfig:    "fake-jit",
		State:        "provisioning",
	}
	if err := s.InsertRunner(r); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := s.GetRunner(r.InstanceName)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != "provisioning" {
		t.Errorf("state = %q, want provisioning", got.State)
	}

	if err := s.UpdateState(r.InstanceName, "running"); err != nil {
		t.Fatalf("update state: %v", err)
	}
	if err := s.UpdateGuestIP(r.InstanceName, "192.168.64.5"); err != nil {
		t.Fatalf("update guest ip: %v", err)
	}
	if err := s.UpdateGitHubRunnerID(r.InstanceName, 42); err != nil {
		t.Fatalf("update runner id: %v", err)
	}

	got, err = s.GetRunner(r.InstanceName)
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got.State != "running" {
		t.Errorf("state = %q, want running", got.State)
	}
	if got.GuestIPString() != "192.168.64.5" {
		t.Errorf("guest ip = %q, want 192.168.64.5", got.GuestIPString())
	}
	if !got.GitHubRunnerID.Valid || got.GitHubRunnerID.Int64 != 42 {
		t.Errorf("runner id = %v, want 42", got.GitHubRunnerID)
	}

	active, err := s.ListActive()
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("active count = %d, want 1", len(active))
	}

	running, err := s.ListActiveByState("running")
	if err != nil {
		t.Fatalf("list by state: %v", err)
	}
	if len(running) != 1 {
		t.Fatalf("running count = %d, want 1", len(running))
	}

	if err := s.SoftDelete(r.InstanceName); err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	active, err = s.ListActive()
	if err != nil {
		t.Fatalf("list active after delete: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("active count after delete = %d, want 0", len(active))
	}

	got, err = s.GetRunner(r.InstanceName)
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if !got.DeletedAt.Valid {
		t.Error("expected deleted_at to be set")
	}

	// HardPruneBefore should keep the row since it's fresh.
	kept, err := s.HardPruneBefore(time.Now().UTC().Add(-1 * time.Hour))
	if err != nil {
		t.Fatalf("hard prune fresh: %v", err)
	}
	if kept != 0 {
		t.Fatalf("pruned %d fresh row(s), expected 0", kept)
	}

	// Manually age the deleted_at so it looks old.
	_, err = s.db.Exec(`UPDATE runners SET deleted_at = ? WHERE instance_name = ?`, time.Now().UTC().Add(-30*24*time.Hour), r.InstanceName)
	if err != nil {
		t.Fatalf("age deleted_at: %v", err)
	}
	pruned, err := s.HardPruneBefore(time.Now().UTC().Add(-7 * 24 * time.Hour))
	if err != nil {
		t.Fatalf("hard prune old: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("pruned %d old row(s), expected 1", pruned)
	}
}
