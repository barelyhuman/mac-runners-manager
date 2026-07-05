package scheduler

import (
	"context"
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
	Boot(ctx context.Context, instanceName string, payload BootPayload) error
	IsRunning(ctx context.Context, instanceName string) (bool, error)
	Stop(ctx context.Context, instanceName string) error
	Delete(ctx context.Context, instanceName string) error
	List(ctx context.Context) ([]string, error)
}

// RunnerRegistrar bridges GitHub's JIT config API to the scheduler.
type RunnerRegistrar interface {
	GenerateJITConfig(ctx context.Context, target TargetRef, runnerName string) (BootPayload, error)
}

// RunnerStatus reports what GitHub currently sees for a named self-hosted
// runner. Found is false if no runner with that name is registered yet
// (e.g. the guest hasn't read its jitconfig and registered at all).
type RunnerStatus struct {
	Found  bool
	Online bool
	Busy   bool
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
