package jsconfig

import (
	"context"
	"os"
	"testing"

	"github.com/usebruno/mac-action-agent/internal/scheduler"
)

func TestLoad_ValidConfig(t *testing.T) {
	cfg, err := Load("testdata/valid.config.js")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(cfg.Targets) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(cfg.Targets))
	}
	if cfg.Targets[0].Owner != "acme" || cfg.Targets[0].Repo != "repo1" {
		t.Errorf("unexpected target[0]: %+v", cfg.Targets[0])
	}
	if len(cfg.Targets[0].Labels) != 3 {
		t.Errorf("expected 3 labels on target[0], got %v", cfg.Targets[0].Labels)
	}
	if cfg.PoolSize != 3 {
		t.Errorf("PoolSize = %d, want 3", cfg.PoolSize)
	}
	if cfg.TickInterval.Seconds() != 45 {
		t.Errorf("TickInterval = %v, want 45s", cfg.TickInterval)
	}

	os.Setenv("TEST_PAT", "abc123")
	defer os.Unsetenv("TEST_PAT")
	pat, err := cfg.Auth(context.Background())
	if err != nil {
		t.Fatalf("Auth: %v", err)
	}
	if pat != "abc123" {
		t.Errorf("Auth() = %q, want abc123", pat)
	}

	if cfg.Priority == nil {
		t.Fatal("expected Priority to be set")
	}
	weights, err := cfg.Priority(scheduler.SchedulerState{
		Targets: []scheduler.TargetDemand{
			{Owner: "acme", Repo: "repo1", QueuedJobs: 3},
			{Owner: "acme", Repo: "repo2", QueuedJobs: 1},
		},
		FreeVMCount: 2,
	})
	if err != nil {
		t.Fatalf("Priority: %v", err)
	}
	if weights["acme/repo1"] != 6 {
		t.Errorf("weights[acme/repo1] = %v, want 6", weights["acme/repo1"])
	}
	if weights["acme/repo2"] != 2 {
		t.Errorf("weights[acme/repo2] = %v, want 2", weights["acme/repo2"])
	}
}

func TestLoad_MinimalConfigUsesDefaults(t *testing.T) {
	cfg, err := Load("testdata/minimal.config.js")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PoolSize != defaultPoolSize {
		t.Errorf("PoolSize = %d, want default %d", cfg.PoolSize, defaultPoolSize)
	}
	if cfg.TickInterval != defaultTickInterval {
		t.Errorf("TickInterval = %v, want default %v", cfg.TickInterval, defaultTickInterval)
	}
	if cfg.Priority != nil {
		t.Error("expected nil Priority when not configured")
	}
	if cfg.ForceSpawn {
		t.Error("expected ForceSpawn to default to false")
	}

	pat, err := cfg.Auth(context.Background())
	if err != nil {
		t.Fatalf("Auth: %v", err)
	}
	if pat != "static-pat-value" {
		t.Errorf("Auth() = %q", pat)
	}
}

func TestLoad_ForceSpawnConfig(t *testing.T) {
	cfg, err := Load("testdata/force_spawn.config.js")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.ForceSpawn {
		t.Error("expected ForceSpawn to be true")
	}
}

func TestLoad_MissingTargetsErrors(t *testing.T) {
	_, err := Load("testdata/missing_targets.config.js")
	if err == nil {
		t.Fatal("expected error for missing targets")
	}
}

func TestLoad_MissingAuthErrors(t *testing.T) {
	_, err := Load("testdata/missing_auth.config.js")
	if err == nil {
		t.Fatal("expected error for missing auth()")
	}
}

func TestLoad_MalformedScriptErrors(t *testing.T) {
	_, err := Load("testdata/malformed.config.js")
	if err == nil {
		t.Fatal("expected error for malformed script")
	}
}

func TestLoad_NonexistentFileErrors(t *testing.T) {
	_, err := Load("testdata/does-not-exist.config.js")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestAuth_ThrowingFunctionReturnsError(t *testing.T) {
	cfg, err := Load("testdata/throwing_auth.config.js")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	_, err = cfg.Auth(context.Background())
	if err == nil {
		t.Fatal("expected error when auth() throws")
	}
}

func TestAuth_ExecFailureReturnsError(t *testing.T) {
	cfg, err := Load("testdata/bad_exec.config.js")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	_, err = cfg.Auth(context.Background())
	if err == nil {
		t.Fatal("expected error when exec() fails")
	}
}
