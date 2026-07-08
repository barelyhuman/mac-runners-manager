package scheduler

import "time"

// VMState is a node in the VM lifecycle state machine:
//
//	Idle -> Provisioning(target) -> Running -> Draining -> Idle
//	                             \-> Failed -/
type VMState int

const (
	Idle VMState = iota
	Provisioning
	Running
	Draining
	Failed
)

func (s VMState) String() string {
	switch s {
	case Idle:
		return "Idle"
	case Provisioning:
		return "Provisioning"
	case Running:
		return "Running"
	case Draining:
		return "Draining"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// VM is one slot in the fixed-size pool this agent manages.
type VM struct {
	InstanceName string
	State        VMState
	Target       *TargetRef
	AssignedAt   time.Time

	// GitHubOfflineSince tracks how long GitHub has reported this VM's
	// runner as offline (or missing) while the tart process is still
	// alive, so a runner that registered then died can be distinguished
	// from a transient status blip before draining it. Zero means "not
	// currently observed offline."
	GitHubOfflineSince time.Time

	// RunnerIdleSince tracks how long GitHub has reported this VM's
	// runner as online but not busy. Zero means "not currently observed
	// idle." Used to reclaim VMs whose runner has finished its job but
	// the VM hypervisor process is still alive.
	RunnerIdleSince time.Time

	// RetryCount counts how many times we've regenerated a JIT config
	// and retried launching the runner inside this VM. Used to bound
	// provisioning retries before giving up.
	RetryCount int

	// RunnerLaunched is true once the agent has successfully started
	// run.sh inside the guest for the current provisioning cycle.
	RunnerLaunched bool

	// RunnerLaunchedAt records when run.sh was started inside the guest.
	// Used to decide when a JIT config has likely expired.
	RunnerLaunchedAt time.Time

	// GuestIP is the resolved IP address of the VM, set once the guest
	// acquires an address and is reachable over SSH.
	GuestIP string

	// JITConfig holds the most recently generated base64-encoded JIT config
	// for this VM. It is written to the guest over SSH when the runner is
	// launched, and regenerated if it expires before the runner comes online.
	JITConfig string
}
