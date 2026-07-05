package scheduler

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	provisioningTimeout = 2 * time.Minute
	instanceNamePrefix  = "mac-action-agent-"

	// githubOfflineGrace bounds how long a Running VM may be reported
	// offline/missing by GitHub before it's drained. Short transient
	// blips (a slow status update after a job finishes) shouldn't cause
	// a healthy runner to be torn down mid-flight.
	githubOfflineGrace = 1 * time.Minute
)

// idGenerator produces unique instance name suffixes. Overridable in tests
// for deterministic output.
type idGenerator func() string

// Scheduler owns the fixed-size VM pool and runs the periodic
// demand -> allocation -> provisioning loop.
type Scheduler struct {
	demand       DemandSource
	provisioner  VMProvisioner
	registrar    RunnerRegistrar
	runnerStatus RunnerStatusChecker
	auth         func(ctx context.Context) (string, error)
	priority    PriorityFunc
	targets     []TargetRef
	baseImage   string
	tickEvery   time.Duration
	genID       idGenerator
	debug       *log.Logger
	forceSpawn  bool

	mu  sync.Mutex
	vms []*VM
}

// Config bundles the dependencies and settings needed to construct a
// Scheduler.
type Config struct {
	Demand       DemandSource
	Provisioner  VMProvisioner
	Registrar    RunnerRegistrar
	RunnerStatus RunnerStatusChecker
	Targets      []TargetRef
	Priority    PriorityFunc
	PoolSize    int
	TickEvery   time.Duration
	BaseImage   string
	// Debug receives verbose tracing of the tick loop's decisions. Nil
	// disables debug logging.
	Debug *log.Logger
	// ForceSpawn, when true and exactly one target is configured, fills the
	// entire idle pool with runners for that target once at startup,
	// bypassing queued-job demand entirely. Ignored with multiple targets.
	ForceSpawn bool
}

// New constructs a Scheduler with a fixed-size pool of idle VM slots.
func New(cfg Config) *Scheduler {
	vms := make([]*VM, cfg.PoolSize)
	for i := range vms {
		vms[i] = &VM{State: Idle}
	}
	debug := cfg.Debug
	if debug == nil {
		debug = log.New(io.Discard, "", 0)
	}
	return &Scheduler{
		demand:       cfg.Demand,
		provisioner:  cfg.Provisioner,
		registrar:    cfg.Registrar,
		runnerStatus: cfg.RunnerStatus,
		priority:     cfg.Priority,
		targets:      cfg.Targets,
		baseImage:    cfg.BaseImage,
		tickEvery:    cfg.TickEvery,
		vms:          vms,
		genID:        defaultIDGenerator,
		debug:        debug,
		forceSpawn:   cfg.ForceSpawn,
	}
}

func defaultIDGenerator() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

// Run starts the scheduler loop. It blocks until ctx is cancelled, at which
// point it stops ticking, gracefully tears down every non-Idle VM (Tart's
// vmnet networking leaves stale bridge/ARP state behind if a VM is killed
// without going through `tart stop`), and returns.
func (s *Scheduler) Run(ctx context.Context) error {
	s.ReconcileOnStartup(ctx)

	if s.forceSpawn {
		s.forceSpawnOnStartup(ctx)
	}

	ticker := time.NewTicker(s.tickEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.shutdown()
			return nil
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil {
				log.Printf("scheduler: tick error: %v", err)
			}
		}
	}
}

// shutdown gracefully stops and deletes every non-Idle VM. It uses a fresh
// context since the one passed to Run has already been cancelled by the
// time this runs.
func (s *Scheduler) shutdown() {
	s.mu.Lock()
	snapshot := make([]*VM, len(s.vms))
	copy(snapshot, s.vms)
	s.mu.Unlock()

	ctx := context.Background()
	for _, vm := range snapshot {
		if vm.State == Idle {
			continue
		}
		log.Printf("scheduler: shutting down, draining %s (was %s)", vm.InstanceName, vm.State)
		s.drain(ctx, vm)
	}
}

// ReconcileOnStartup adopts any VMs already running under our naming prefix
// (e.g. after a crash/restart) as Running-with-unknown-target, rather than
// killing them — they may be serving an in-flight job.
func (s *Scheduler) ReconcileOnStartup(ctx context.Context) {
	names, err := s.provisioner.List(ctx)
	if err != nil {
		log.Printf("scheduler: startup reconciliation failed to list VMs: %v", err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range names {
		if len(name) <= len(instanceNamePrefix) || name[:len(instanceNamePrefix)] != instanceNamePrefix {
			continue
		}
		for _, vm := range s.vms {
			if vm.State == Idle {
				vm.InstanceName = name
				vm.State = Running
				vm.AssignedAt = time.Now()
				log.Printf("scheduler: adopted orphan VM %s as Running", name)
				break
			}
		}
	}
}

// Tick runs one scheduling cycle: reconcile existing VM states, poll demand,
// apply the priority function, compute an allocation, and kick off
// provisioning for winning targets.
func (s *Scheduler) Tick(ctx context.Context) error {
	s.reconcileVMStates(ctx)

	snapshots, err := s.demand.Poll(ctx, s.targets)
	if err != nil {
		return fmt.Errorf("poll demand: %w", err)
	}
	for _, snap := range snapshots {
		s.debug.Printf("scheduler: demand %s: %d queued job(s)", snap.Target.Key(), snap.QueuedJobs)
	}

	idle := s.idleCount()
	s.debug.Printf("scheduler: %d idle VM(s) in pool", idle)

	var allocation map[string]int
	if len(s.targets) == 1 {
		allocation = s.allocateSingleTarget(idle, snapshots)
		s.debug.Printf("scheduler: single target, skipping priority()/weighting")
	} else {
		demands := s.buildDemands(snapshots)
		allocation = Allocate(idle, demands)
	}
	s.debug.Printf("scheduler: computed allocation: %s", formatAllocation(allocation))

	return s.applyAllocation(ctx, allocation)
}

// formatAllocation renders an allocation map in a stable, human-readable
// order for debug logging.
func formatAllocation(allocation map[string]int) string {
	if len(allocation) == 0 {
		return "(none)"
	}
	keys := make([]string, 0, len(allocation))
	for k := range allocation {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, allocation[k]))
	}
	return fmt.Sprintf("%v", parts)
}

// forceSpawnOnStartup immediately provisions every idle VM in the pool for
// the single configured target, ignoring queued-job demand entirely. This is
// a one-shot startup action for configs where GitHub's queued-job signal
// can't be trusted to fill the pool on its own; normal demand-based
// allocation resumes on subsequent ticks. No-op with zero or multiple
// targets.
func (s *Scheduler) forceSpawnOnStartup(ctx context.Context) {
	if len(s.targets) != 1 {
		s.debug.Printf("scheduler: forceSpawn is only supported with exactly one target, skipping")
		return
	}
	idle := s.idleCount()
	if idle <= 0 {
		return
	}
	target := s.targets[0]
	s.debug.Printf("scheduler: forceSpawn enabled, provisioning %d VM(s) for %s at startup", idle, target.Key())
	if err := s.applyAllocation(ctx, map[string]int{target.Key(): idle}); err != nil {
		log.Printf("scheduler: forceSpawn provisioning failed: %v", err)
	}
}

// allocateSingleTarget skips weighting/apportionment entirely: with only one
// target there's nothing to share the pool with, so it gets all idle
// capacity, still bounded by its own queued job count.
func (s *Scheduler) allocateSingleTarget(idle int, snapshots []DemandSnapshot) map[string]int {
	if idle <= 0 || len(snapshots) == 0 {
		return map[string]int{}
	}
	snap := snapshots[0]
	n := idle
	if snap.QueuedJobs < n {
		n = snap.QueuedJobs
	}
	if n <= 0 {
		return map[string]int{}
	}
	return map[string]int{snap.Target.Key(): n}
}

func (s *Scheduler) buildDemands(snapshots []DemandSnapshot) []Demand {
	weights := s.resolveWeights(snapshots)

	demands := make([]Demand, 0, len(snapshots))
	for _, snap := range snapshots {
		w := weights[snap.Target.Key()]
		demands = append(demands, Demand{
			Target:     snap.Target,
			Weight:     w,
			QueuedJobs: snap.QueuedJobs,
		})
	}
	return demands
}

// resolveWeights returns weight-per-target, using the JS priority() hook if
// configured, falling back to raw QueuedJobs counts otherwise.
func (s *Scheduler) resolveWeights(snapshots []DemandSnapshot) map[string]float64 {
	defaults := make(map[string]float64, len(snapshots))
	for _, snap := range snapshots {
		defaults[snap.Target.Key()] = float64(snap.QueuedJobs)
	}

	if s.priority == nil {
		s.debug.Printf("scheduler: no priority() configured, using default weighting (weight = queuedJobs)")
		return defaults
	}

	state := SchedulerState{
		FreeVMCount: s.idleCount(),
		Targets:     make([]TargetDemand, 0, len(snapshots)),
	}
	for _, snap := range snapshots {
		state.Targets = append(state.Targets, TargetDemand{
			Owner:      snap.Target.Owner,
			Repo:       snap.Target.Repo,
			QueuedJobs: snap.QueuedJobs,
		})
	}

	weights, err := s.priority(state)
	if err != nil {
		log.Printf("scheduler: priority() failed, falling back to default weighting: %v", err)
		return defaults
	}
	if len(weights) == 0 {
		s.debug.Printf("scheduler: priority() returned no weights, using default weighting")
		return defaults
	}
	s.debug.Printf("scheduler: priority() returned weights: %v", weights)
	return weights
}

func (s *Scheduler) idleCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, vm := range s.vms {
		if vm.State == Idle {
			n++
		}
	}
	return n
}

// applyAllocation provisions one idle VM per winning target. Provisioning is
// fire-and-forget for this tick; confirmation happens via reconcileVMStates
// on a later tick, keeping each tick fast and non-blocking.
func (s *Scheduler) applyAllocation(ctx context.Context, allocation map[string]int) error {
	targetsByKey := make(map[string]TargetRef, len(s.targets))
	for _, t := range s.targets {
		targetsByKey[t.Key()] = t
	}

	for key, count := range allocation {
		target, ok := targetsByKey[key]
		if !ok {
			continue
		}
		for i := 0; i < count; i++ {
			vm := s.claimIdleVM()
			if vm == nil {
				s.debug.Printf("scheduler: pool exhausted, could not claim VM %d/%d for %s", i+1, count, key)
				break // pool exhausted this tick
			}
			s.debug.Printf("scheduler: provisioning VM for %s", key)
			if err := s.provision(ctx, vm, target); err != nil {
				log.Printf("scheduler: provisioning for %s failed: %v", key, err)
				s.mu.Lock()
				vm.State = Failed
				s.mu.Unlock()
			}
		}
	}
	return nil
}

// claimIdleVM atomically finds and reserves one Idle VM, marking it
// Provisioning with no target yet assigned (set by the caller next).
func (s *Scheduler) claimIdleVM() *VM {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, vm := range s.vms {
		if vm.State == Idle {
			vm.State = Provisioning
			return vm
		}
	}
	return nil
}

// maxInstanceNameLen matches GitHub's runner name limit (64 chars), which is
// stricter than any Tart VM naming constraint, so it governs the shared name
// used for both.
const maxInstanceNameLen = 64

// buildInstanceName derives a name safe to use as both a Tart VM name and a
// GitHub runner name: alphanumerics/hyphens only, <=64 chars. GitHub rejects
// '/' (present in TargetRef.Key()) and names over 64 chars.
func buildInstanceName(target TargetRef, id string) string {
	sanitizedKey := targetNameSanitizer.Replace(target.Key())
	suffix := "-" + id
	prefixBudget := maxInstanceNameLen - len(instanceNamePrefix) - len(suffix)
	if prefixBudget < 0 {
		prefixBudget = 0
	}
	if len(sanitizedKey) > prefixBudget {
		sanitizedKey = sanitizedKey[:prefixBudget]
	}
	return instanceNamePrefix + sanitizedKey + suffix
}

var targetNameSanitizer = strings.NewReplacer("/", "-")

func (s *Scheduler) provision(ctx context.Context, vm *VM, target TargetRef) error {
	instanceName := buildInstanceName(target, s.genID())
	s.debug.Printf("scheduler: %s: generating JIT config", instanceName)

	payload, err := s.registrar.GenerateJITConfig(ctx, target, instanceName)
	if err != nil {
		return fmt.Errorf("generate JIT config: %w", err)
	}

	s.debug.Printf("scheduler: %s: cloning from base image %s", instanceName, s.baseImage)
	if err := s.provisioner.Clone(ctx, s.baseImage, instanceName); err != nil {
		return fmt.Errorf("clone VM: %w", err)
	}

	s.debug.Printf("scheduler: %s: booting", instanceName)
	if err := s.provisioner.Boot(ctx, instanceName, payload); err != nil {
		return fmt.Errorf("boot VM: %w", err)
	}

	s.mu.Lock()
	vm.InstanceName = instanceName
	vm.Target = &target
	vm.AssignedAt = time.Now()
	s.mu.Unlock()

	s.debug.Printf("scheduler: %s: boot command issued, will confirm via IsRunning on a later tick", instanceName)
	return nil
}

// reconcileVMStates checks Provisioning and Running VMs against ground
// truth: a Provisioning VM that's now alive moves to Running (or Failed on
// timeout); a Running VM whose ephemeral runner process has exited (JIT
// runners self-deregister after one job) moves to Draining, gets torn down,
// and returns to Idle. Idle VMs are never touched here.
func (s *Scheduler) reconcileVMStates(ctx context.Context) {
	s.mu.Lock()
	snapshot := make([]*VM, len(s.vms))
	copy(snapshot, s.vms)
	s.mu.Unlock()

	for _, vm := range snapshot {
		switch vm.State {
		case Provisioning:
			s.reconcileProvisioning(ctx, vm)
		case Running:
			s.reconcileRunning(ctx, vm)
		case Failed:
			s.reconcileFailed(ctx, vm)
		}
	}
}

func (s *Scheduler) reconcileProvisioning(ctx context.Context, vm *VM) {
	running, err := s.provisioner.IsRunning(ctx, vm.InstanceName)
	if err != nil {
		log.Printf("scheduler: IsRunning check failed for %s: %v", vm.InstanceName, err)
		return
	}
	if !running {
		s.debug.Printf("scheduler: %s: tart process not yet up (%s elapsed)", vm.InstanceName, time.Since(vm.AssignedAt))
		s.failIfProvisioningTimedOut(vm)
		return
	}

	online, err := s.checkRunnerOnline(ctx, vm)
	if err != nil {
		log.Printf("scheduler: runner status check failed for %s: %v", vm.InstanceName, err)
		return
	}
	if !online {
		s.debug.Printf("scheduler: %s: tart alive, runner not yet online on GitHub (%s elapsed)", vm.InstanceName, time.Since(vm.AssignedAt))
		s.failIfProvisioningTimedOut(vm)
		return
	}

	s.debug.Printf("scheduler: %s: confirmed running (tart alive, GitHub online)", vm.InstanceName)
	s.mu.Lock()
	vm.State = Running
	s.mu.Unlock()
}

// failIfProvisioningTimedOut marks vm Failed once provisioningTimeout has
// elapsed since it was claimed, regardless of which signal (tart, GitHub)
// never came up in time.
func (s *Scheduler) failIfProvisioningTimedOut(vm *VM) {
	if time.Since(vm.AssignedAt) > provisioningTimeout {
		log.Printf("scheduler: %s failed to come up within %s, marking Failed", vm.InstanceName, provisioningTimeout)
		s.mu.Lock()
		vm.State = Failed
		s.mu.Unlock()
	}
}

// checkRunnerOnline reports whether GitHub currently shows an online runner
// for vm. If no RunnerStatusChecker is configured, it degrades to "true"
// (tart-alive-only behavior), so callers that don't wire one up keep the
// prior behavior rather than getting stuck forever.
func (s *Scheduler) checkRunnerOnline(ctx context.Context, vm *VM) (bool, error) {
	if s.runnerStatus == nil || vm.Target == nil {
		return true, nil
	}
	status, err := s.runnerStatus.RunnerStatus(ctx, *vm.Target, vm.InstanceName)
	if err != nil {
		return false, err
	}
	return status.Found && status.Online, nil
}

func (s *Scheduler) reconcileRunning(ctx context.Context, vm *VM) {
	running, err := s.provisioner.IsRunning(ctx, vm.InstanceName)
	if err != nil {
		log.Printf("scheduler: IsRunning check failed for %s: %v", vm.InstanceName, err)
		return
	}
	if !running {
		// The ephemeral runner process has exited (job finished, self-deregistered).
		s.mu.Lock()
		vm.State = Draining
		s.mu.Unlock()
		s.drain(ctx, vm)
		return
	}

	online, err := s.checkRunnerOnline(ctx, vm)
	if err != nil {
		log.Printf("scheduler: runner status check failed for %s: %v", vm.InstanceName, err)
		return
	}
	if online {
		if !vm.GitHubOfflineSince.IsZero() {
			s.debug.Printf("scheduler: %s: runner back online on GitHub", vm.InstanceName)
			s.mu.Lock()
			vm.GitHubOfflineSince = time.Time{}
			s.mu.Unlock()
		}
		return
	}

	s.mu.Lock()
	if vm.GitHubOfflineSince.IsZero() {
		vm.GitHubOfflineSince = time.Now()
	}
	offlineFor := time.Since(vm.GitHubOfflineSince)
	s.mu.Unlock()

	s.debug.Printf("scheduler: %s: tart alive but GitHub reports offline/missing (%s)", vm.InstanceName, offlineFor)
	if offlineFor > githubOfflineGrace {
		log.Printf("scheduler: %s reported offline by GitHub for over %s, draining", vm.InstanceName, githubOfflineGrace)
		s.mu.Lock()
		vm.State = Draining
		s.mu.Unlock()
		s.drain(ctx, vm)
	}
}

func (s *Scheduler) reconcileFailed(ctx context.Context, vm *VM) {
	s.drain(ctx, vm)
}

// drain tears down a VM's ephemeral resources and returns its slot to Idle.
func (s *Scheduler) drain(ctx context.Context, vm *VM) {
	if vm.InstanceName != "" {
		if err := s.provisioner.Stop(ctx, vm.InstanceName); err != nil {
			log.Printf("scheduler: stop %s failed: %v", vm.InstanceName, err)
		}
		if err := s.provisioner.Delete(ctx, vm.InstanceName); err != nil {
			log.Printf("scheduler: delete %s failed: %v", vm.InstanceName, err)
		}
	}

	s.mu.Lock()
	vm.State = Idle
	vm.InstanceName = ""
	vm.Target = nil
	vm.AssignedAt = time.Time{}
	vm.GitHubOfflineSince = time.Time{}
	s.mu.Unlock()
}
