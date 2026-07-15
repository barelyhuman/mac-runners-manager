package scheduler

import (
	"context"
	"io"
	"time"
)

// TargetRef identifies a single GitHub repo this agent watches and can
// register runners against.
type TargetRef struct {
	Owner  string
	Repo   string
	Labels []string
}

// Key returns the "owner/repo" identifier used as a map key throughout the
// scheduler and in JS-facing priority weights.
func (t TargetRef) Key() string {
	return t.Owner + "/" + t.Repo
}

// DemandSnapshot is what a DemandSource reports for one target at one tick.
type DemandSnapshot struct {
	Target     TargetRef
	QueuedJobs int
	ObservedAt time.Time
}

// DemandSource abstracts how the scheduler learns about queued work. Polling
// is the only MVP implementation; a webhook-fed implementation could satisfy
// the same interface later.
type DemandSource interface {
	Poll(ctx context.Context, targets []TargetRef) ([]DemandSnapshot, error)
}

// BootPayload carries everything a VM needs injected at boot to self-register
// as a GitHub Actions runner.
type BootPayload struct {
	JITConfig string
	ExtraEnv  map[string]string
}

// VMProvisioner is the Tart control-plane abstraction.
type VMProvisioner interface {
	Clone(ctx context.Context, baseImage, instanceName string) error
	SetMemory(ctx context.Context, instanceName string, memoryMB int) error
	Boot(ctx context.Context, instanceName string, payload BootPayload) error
	IsRunning(ctx context.Context, instanceName string) (bool, error)
	Stop(ctx context.Context, instanceName string) error
	Delete(ctx context.Context, instanceName string) error
	List(ctx context.Context) ([]string, error)
	// IP resolves the guest IP of a running instance, waiting up to
	// waitSeconds for networking to come up.
	IP(ctx context.Context, instanceName string, waitSeconds int) (string, error)
}

// GuestRunnerProvisioner orchestrates the installation and startup of a
// GitHub Actions runner inside a VM over SSH.
type GuestRunnerProvisioner interface {
	// IsInstalled reports whether the runner is already present in the guest.
	IsInstalled(ctx context.Context, ip string) (bool, error)
	// Install downloads and extracts the GitHub Actions runner for the
	// requested version tag (empty = latest).
	Install(ctx context.Context, ip, versionTag string) error
	// Version returns the installed runner version or an empty string.
	Version(ctx context.Context, ip string) (string, error)
	// WriteJITConfig writes the base64-encoded JIT config to a temp file
	// inside the guest and returns the guest-side path.
	WriteJITConfig(ctx context.Context, ip, jitConfig string) (string, error)
	// StartRunner launches run.sh --jitconfig inside the guest in the
	// background (via nohup).
	StartRunner(ctx context.Context, ip, jitConfigPath string) error
	// KillRunner terminates any existing run.sh process in the guest.
	KillRunner(ctx context.Context, ip string) error
	// RemoveRunner force-stops the runner and deletes its config files.
	// Used as a fallback when the GitHub API path is unavailable.
	RemoveRunner(ctx context.Context, ip string) error
	// TailLogs returns a ReadCloser streaming the runner diag logs.
	// The caller is responsible for closing it.
	TailLogs(ctx context.Context, ip string) (io.ReadCloser, error)
}

// RunnerCleaner force-removes a stale self-hosted runner registration from
// GitHub so that a fresh JIT config can be minted for the same VM.
type RunnerCleaner interface {
	DeleteRunnerByName(ctx context.Context, target TargetRef, runnerName string) error
}

// RunnerRegistrar bridges GitHub's JIT config API to the scheduler.
type RunnerRegistrar interface {
	GenerateJITConfig(ctx context.Context, target TargetRef, runnerName string) (BootPayload, error)
}

// RunnerStatus reports what GitHub currently sees for a named self-hosted
// runner. Found is false if no runner with that name is registered yet
// (e.g. the guest hasn't read its jitconfig and registered at all).
type RunnerStatus struct {
	Found    bool
	Online   bool
	Busy     bool
	RunnerID int64 // cached once discovered via the list-runners API
}

// RunnerStatusChecker bridges GitHub's list-runners API to the scheduler,
// used to confirm a runner actually came online inside a booted VM rather
// than trusting the VM hypervisor process's alive bit alone.
type RunnerStatusChecker interface {
	RunnerStatus(ctx context.Context, target TargetRef, runnerName string) (RunnerStatus, error)
}

// SchedulerState is the read-only view handed to the JS priority() hook.
// JSON tags double as the goja field-name mapping so JS sees camelCase keys
// (state.targets, state.freeVmCount) instead of Go's exported names.
type SchedulerState struct {
	Targets     []TargetDemand `json:"targets"`
	FreeVMCount int            `json:"freeVmCount"`
}

type TargetDemand struct {
	Owner      string `json:"owner"`
	Repo       string `json:"repo"`
	QueuedJobs int    `json:"queuedJobs"`
}

// PriorityFunc is the JS-configurable allocation hook. It returns a weight
// per "owner/repo" key; an empty/nil map means "use default weighting"
// (weight = QueuedJobs).
type PriorityFunc func(state SchedulerState) (map[string]float64, error)
