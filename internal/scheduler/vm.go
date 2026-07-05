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
}
