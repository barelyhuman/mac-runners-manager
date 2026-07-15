package scheduler

import (
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDemandSource returns a canned set of snapshots on each Poll call.
type fakeDemandSource struct {
	mu        sync.Mutex
	snapshots []DemandSnapshot
	err       error
}

func (f *fakeDemandSource) Poll(ctx context.Context, targets []TargetRef) ([]DemandSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshots, f.err
}

func (f *fakeDemandSource) setQueued(counts map[string]int, targets []TargetRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	snaps := make([]DemandSnapshot, 0, len(targets))
	for _, t := range targets {
		snaps = append(snaps, DemandSnapshot{Target: t, QueuedJobs: counts[t.Key()]})
	}
	f.snapshots = snaps
}

// fakeProvisioner tracks VM instances and lets tests control IsRunning
// responses per instance name to simulate boot confirmation and job
// completion.
type fakeProvisioner struct {
	mu         sync.Mutex
	cloned     []string
	memorySet  map[string]int
	booted     []string
	stopped    []string
	deleted    []string
	running    map[string]bool
	cloneErr   error
	bootErr    error
	memoryErr  error
}

func newFakeProvisioner() *fakeProvisioner {
	return &fakeProvisioner{running: map[string]bool{}, memorySet: map[string]int{}}
}

func (f *fakeProvisioner) Clone(ctx context.Context, baseImage, instanceName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cloneErr != nil {
		return f.cloneErr
	}
	f.cloned = append(f.cloned, instanceName)
	return nil
}

func (f *fakeProvisioner) SetMemory(ctx context.Context, instanceName string, memoryMB int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.memoryErr != nil {
		return f.memoryErr
	}
	f.memorySet[instanceName] = memoryMB
	return nil
}

func (f *fakeProvisioner) Boot(ctx context.Context, instanceName string, payload BootPayload) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bootErr != nil {
		return f.bootErr
	}
	f.booted = append(f.booted, instanceName)
	f.running[instanceName] = true // becomes running immediately for test simplicity
	return nil
}

func (f *fakeProvisioner) IsRunning(ctx context.Context, instanceName string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.running[instanceName], nil
}

func (f *fakeProvisioner) Stop(ctx context.Context, instanceName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, instanceName)
	return nil
}

func (f *fakeProvisioner) Delete(ctx context.Context, instanceName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, instanceName)
	delete(f.running, instanceName)
	return nil
}

func (f *fakeProvisioner) List(ctx context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for name, running := range f.running {
		if running {
			names = append(names, name)
		}
	}
	return names, nil
}

func (f *fakeProvisioner) setRunning(instanceName string, running bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running[instanceName] = running
}

func (f *fakeProvisioner) IP(ctx context.Context, instanceName string, waitSeconds int) (string, error) {
	return "192.0.2.1", nil
}

// fakeGuestProvisioner simulates a working SSH-based runner setup inside
// the guest. All operations succeed instantly.
type fakeGuestProvisioner struct{}

func (f *fakeGuestProvisioner) IsInstalled(ctx context.Context, ip string) (bool, error)   { return true, nil }
func (f *fakeGuestProvisioner) Install(ctx context.Context, ip, versionTag string) error   { return nil }
func (f *fakeGuestProvisioner) Version(ctx context.Context, ip string) (string, error)     { return "2.0.0", nil }
func (f *fakeGuestProvisioner) WriteJITConfig(ctx context.Context, ip, jitConfig string) (string, error) {
	return "/tmp/.jitconfig", nil
}
func (f *fakeGuestProvisioner) StartRunner(ctx context.Context, ip, jitConfigPath string) error { return nil }
func (f *fakeGuestProvisioner) KillRunner(ctx context.Context, ip string) error               { return nil }
func (f *fakeGuestProvisioner) RemoveRunner(ctx context.Context, ip string) error              { return nil }
func (f *fakeGuestProvisioner) TailLogs(ctx context.Context, ip string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

// fakeRegistrar always succeeds with a canned payload.
type fakeRegistrar struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeRegistrar) GenerateJITConfig(ctx context.Context, target TargetRef, runnerName string) (BootPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return BootPayload{JITConfig: "fake-jit-config"}, nil
}

// fakeRunnerStatusChecker lets tests control what GitHub reports for a
// given runner name, defaulting to "not found" for anything unset.
type fakeRunnerStatusChecker struct {
	mu       sync.Mutex
	statuses map[string]RunnerStatus
}

func newFakeRunnerStatusChecker() *fakeRunnerStatusChecker {
	return &fakeRunnerStatusChecker{statuses: map[string]RunnerStatus{}}
}

func (f *fakeRunnerStatusChecker) RunnerStatus(ctx context.Context, target TargetRef, runnerName string) (RunnerStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statuses[runnerName], nil
}

func (f *fakeRunnerStatusChecker) setOnline(runnerName string, online bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[runnerName] = RunnerStatus{Found: online, Online: online}
}

func (f *fakeRunnerStatusChecker) setOnlineAndBusy(runnerName string, online, busy bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[runnerName] = RunnerStatus{Found: online, Online: online, Busy: busy}
}

func testScheduler(t *testing.T, poolSize int, targets []TargetRef, ds *fakeDemandSource, prov *fakeProvisioner, reg *fakeRegistrar, opts ...func(*Config)) *Scheduler {
	t.Helper()
	cfg := Config{
		Demand:      ds,
		Provisioner: prov,
		Registrar:   reg,
		GuestRunner: &fakeGuestProvisioner{},
		Targets:     targets,
		PoolSize:    poolSize,
		TickEvery:   time.Hour,
		BaseImage:   "test-base-image",
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	s := New(cfg)
	seq := 0
	s.genID = func() string {
		seq++
		return string(rune('a' + seq - 1))
	}
	return s
}

func withRunnerStatus(rs RunnerStatusChecker) func(*Config) {
	return func(c *Config) { c.RunnerStatus = rs }
}

func TestTick_ProvisionsIdleVMsForDemand(t *testing.T) {
	targets := []TargetRef{
		{Owner: "acme", Repo: "repo1"},
		{Owner: "acme", Repo: "repo2"},
	}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 2, "acme/repo2": 5}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 2, targets, ds, prov, reg)

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if len(prov.cloned) != 2 {
		t.Fatalf("expected 2 VMs cloned, got %d: %v", len(prov.cloned), prov.cloned)
	}
	if reg.calls != 2 {
		t.Errorf("expected 2 JIT config calls, got %d", reg.calls)
	}

	// Both repos should have received exactly 1 VM (guarantee-phase fairness).
	s.mu.Lock()
	byTarget := map[string]int{}
	for _, vm := range s.vms {
		if vm.Target != nil {
			byTarget[vm.Target.Key()]++
		}
	}
	s.mu.Unlock()
	if byTarget["acme/repo1"] != 1 || byTarget["acme/repo2"] != 1 {
		t.Errorf("expected 1 VM each, got %v", byTarget)
	}
}

func TestTick_NoIdleVMsSkipsProvisioning(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 5}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 0, targets, ds, prov, reg)
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(prov.cloned) != 0 {
		t.Errorf("expected no VMs cloned with zero pool, got %v", prov.cloned)
	}
}

func TestTick_ZeroDemandProvisionsNothing(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 2, targets, ds, prov, reg)
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(prov.cloned) != 0 {
		t.Errorf("expected no VMs cloned with zero demand, got %v", prov.cloned)
	}
}

func TestFullLifecycle_ProvisioningToRunningToIdle(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 1, targets, ds, prov, reg)

	// Tick 1: provisions the only idle VM.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	s.mu.Lock()
	instanceName := s.vms[0].InstanceName
	state := s.vms[0].State
	s.mu.Unlock()
	if state != Provisioning {
		t.Fatalf("expected VM Provisioning right after Tick 1, got %v", state)
	}
	if instanceName == "" {
		t.Fatal("expected instance name to be set")
	}

	// No more demand - nothing new should provision on subsequent ticks.
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)

	// Tick 2: reconcileVMStates resolves IP and launches runner inside guest.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}
	s.mu.Lock()
	state = s.vms[0].State
	s.mu.Unlock()
	if state != Provisioning {
		t.Fatalf("expected VM Provisioning while runner launching, got %v", state)
	}

	// Tick 3: reconcileVMStates confirms runner is online on GitHub.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 3: %v", err)
	}
	s.mu.Lock()
	state = s.vms[0].State
	s.mu.Unlock()
	if state != Running {
		t.Fatalf("expected VM Running after Tick 3 reconciliation, got %v", state)
	}

	// Simulate the ephemeral runner finishing its one job and deregistering.
	prov.setRunning(instanceName, false)

	// Tick 4: reconcileVMStates notices the runner exited, drains, returns to Idle.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 4: %v", err)
	}

	s.mu.Lock()
	finalState := s.vms[0].State
	finalName := s.vms[0].InstanceName
	s.mu.Unlock()
	if finalState != Idle {
		t.Errorf("expected VM to return to Idle after draining, got %v", finalState)
	}
	if finalName != "" {
		t.Errorf("expected instance name cleared after drain, got %q", finalName)
	}

	found := false
	for _, n := range prov.stopped {
		if n == instanceName {
			found = true
		}
	}
	if !found {
		t.Errorf("expected %s to be stopped during drain", instanceName)
	}
	found = false
	for _, n := range prov.deleted {
		if n == instanceName {
			found = true
		}
	}
	if !found {
		t.Errorf("expected %s to be deleted during drain", instanceName)
	}
}

func TestTick_ProvisioningFailureMarksFailedThenDrains(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	prov.bootErr = errBoom
	reg := &fakeRegistrar{}

	s := testScheduler(t, 1, targets, ds, prov, reg)

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	s.mu.Lock()
	state := s.vms[0].State
	s.mu.Unlock()
	if state != Failed {
		t.Fatalf("expected Failed state after boot error, got %v", state)
	}

	// Next tick drains the Failed VM back to Idle at the start of the tick,
	// then (since demand is still 1) immediately re-provisions and fails
	// again, ending the tick back in Failed. Stop demand to observe the
	// drain-to-Idle transition in isolation.
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}
	s.mu.Lock()
	state = s.vms[0].State
	s.mu.Unlock()
	if state != Idle {
		t.Errorf("expected Failed VM to drain back to Idle, got %v", state)
	}
}

func TestReconcileOnStartup_WarnsOnOrphanVMs(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	prov := newFakeProvisioner()
	prov.setRunning("mac-runners-manager-acme/repo1-orphan123", true)
	reg := &fakeRegistrar{}

	s := testScheduler(t, 2, targets, ds, prov, reg)
	s.ReconcileOnStartup(context.Background())

	s.mu.Lock()
	defer s.mu.Unlock()
	running := 0
	for _, vm := range s.vms {
		if vm.State == Running {
			running++
		}
	}
	if running != 0 {
		t.Errorf("expected 0 adopted VMs, got %d", running)
	}
}

func TestTick_SingleTargetSkipsWeighting(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 3, targets, ds, prov, reg)
	priorityCalls := 0
	s.priority = func(state SchedulerState) (map[string]float64, error) {
		priorityCalls++
		return map[string]float64{"acme/repo1": 1}, nil
	}

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if priorityCalls != 0 {
		t.Errorf("expected priority() not to be called for a single target, got %d calls", priorityCalls)
	}
	if len(prov.cloned) != 1 {
		t.Fatalf("expected 1 VM cloned (clamped to queued jobs), got %d: %v", len(prov.cloned), prov.cloned)
	}
}

func TestTick_SingleTargetGetsFullIdlePoolUpToQueuedJobs(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 10}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 3, targets, ds, prov, reg)

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(prov.cloned) != 3 {
		t.Fatalf("expected all 3 idle VMs assigned to the single target, got %d: %v", len(prov.cloned), prov.cloned)
	}
}

func TestTick_SingleTargetClampedToQueuedJobs(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 2}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 5, targets, ds, prov, reg)

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(prov.cloned) != 2 {
		t.Fatalf("expected VMs clamped to 2 queued jobs, got %d: %v", len(prov.cloned), prov.cloned)
	}
}

func TestRun_ForceSpawnFillsPoolBeforeEnteringTickLoop(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 3, targets, ds, prov, reg)
	s.forceSpawn = true

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Run still does startup work (ReconcileOnStartup, forceSpawn) before selecting on ctx.Done().
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(prov.cloned) != 3 {
		t.Fatalf("expected all 3 idle VMs force-spawned despite zero demand, got %d: %v", len(prov.cloned), prov.cloned)
	}
}

func TestForceSpawnOnStartup_FillsPoolIgnoringDemand(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 3, targets, ds, prov, reg)
	s.forceSpawn = true

	s.forceSpawnOnStartup(context.Background())

	if len(prov.cloned) != 3 {
		t.Fatalf("expected all 3 idle VMs force-spawned despite zero demand, got %d: %v", len(prov.cloned), prov.cloned)
	}
}

func TestForceSpawnOnStartup_NoopWithMultipleTargets(t *testing.T) {
	targets := []TargetRef{
		{Owner: "acme", Repo: "repo1"},
		{Owner: "acme", Repo: "repo2"},
	}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 0, "acme/repo2": 0}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 3, targets, ds, prov, reg)
	s.forceSpawn = true

	s.forceSpawnOnStartup(context.Background())

	if len(prov.cloned) != 0 {
		t.Fatalf("expected forceSpawn to no-op with multiple targets, got %d cloned: %v", len(prov.cloned), prov.cloned)
	}
}

func TestBuildInstanceName_SanitizesSlashAndStaysWithinLimit(t *testing.T) {
	target := TargetRef{Owner: "usebruno", Repo: "bruno"}
	name := buildInstanceName(target, "1783050927689331000")

	if strings.Contains(name, "/") {
		t.Errorf("expected no '/' in instance name, got %q", name)
	}
	if len(name) > maxInstanceNameLen {
		t.Errorf("expected name <= %d chars, got %d: %q", maxInstanceNameLen, len(name), name)
	}
	if !strings.HasPrefix(name, instanceNamePrefix) {
		t.Errorf("expected name to keep prefix %q, got %q", instanceNamePrefix, name)
	}
	if !strings.HasSuffix(name, "-1783050927689331000") {
		t.Errorf("expected id suffix preserved, got %q", name)
	}
}

func TestBuildInstanceName_TruncatesVeryLongRepoNames(t *testing.T) {
	target := TargetRef{
		Owner: "an-organization-with-a-very-long-name-indeed",
		Repo:  "and-an-equally-long-repository-name-to-match-it",
	}
	name := buildInstanceName(target, "abc123")

	if len(name) > maxInstanceNameLen {
		t.Errorf("expected name <= %d chars, got %d: %q", maxInstanceNameLen, len(name), name)
	}
	if !strings.HasSuffix(name, "-abc123") {
		t.Errorf("expected id suffix preserved even when key is truncated, got %q", name)
	}
}

func TestTick_ProvisioningSetsVMMemoryWhenConfigured(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 1, targets, ds, prov, reg, func(c *Config) {
		c.VMMemoryMB = 4096
	})

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if len(prov.cloned) != 1 {
		t.Fatalf("expected 1 VM cloned, got %d", len(prov.cloned))
	}
	instanceName := prov.cloned[0]
	if prov.memorySet[instanceName] != 4096 {
		t.Errorf("expected SetMemory(4096) for %s, got %v", instanceName, prov.memorySet)
	}
}

func TestTick_ProvisioningSkipsSetVMMemoryWhenZero(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 1, targets, ds, prov, reg)

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if len(prov.memorySet) != 0 {
		t.Errorf("expected no SetMemory calls when VMMemoryMB=0, 		got %v", prov.memorySet)
	}
}

func TestProvision_RunnerNamePassedToRegistrarIsValid(t *testing.T) {
	targets := []TargetRef{{Owner: "usebruno", Repo: "bruno"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"usebruno/bruno": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}

	s := testScheduler(t, 1, targets, ds, prov, reg)
	s.genID = func() string { return strconv.FormatInt(1783050927689331000, 36) }

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(prov.cloned) != 1 {
		t.Fatalf("expected 1 VM cloned, got %d: %v", len(prov.cloned), prov.cloned)
	}
	name := prov.cloned[0]
	if strings.Contains(name, "/") || len(name) > maxInstanceNameLen {
		t.Errorf("runner name %q is invalid: contains '/' or exceeds %d chars", name, maxInstanceNameLen)
	}
}

type boomErr string

func (e boomErr) Error() string { return string(e) }

const errBoom = boomErr("boom")

func TestReconcileProvisioning_StaysProvisioningWhileTartAliveButGitHubOffline(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}
	rs := newFakeRunnerStatusChecker() // never marked online

	s := testScheduler(t, 1, targets, ds, prov, reg, withRunnerStatus(rs))

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)

	// tart reports the VM alive (fakeProvisioner.Boot sets this), but GitHub
	// never reports the runner online: the VM must not be promoted to Running.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}
	s.mu.Lock()
	state := s.vms[0].State
	s.mu.Unlock()
	if state != Provisioning {
		t.Fatalf("expected VM to stay Provisioning while GitHub reports offline, got %v", state)
	}
}

func TestReconcileProvisioning_PromotesToRunningOnceGitHubReportsOnline(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}
	rs := newFakeRunnerStatusChecker()

	s := testScheduler(t, 1, targets, ds, prov, reg, withRunnerStatus(rs))

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	s.mu.Lock()
	instanceName := s.vms[0].InstanceName
	s.mu.Unlock()
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)

	// Tick 2: reconcileVMStates resolves IP and launches runner.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}
	s.mu.Lock()
	state := s.vms[0].State
	s.mu.Unlock()
	if state != Provisioning {
		t.Fatalf("expected VM Provisioning while runner launching, got %v", state)
	}

	// Now mark the runner online on GitHub.
	rs.setOnline(instanceName, true)

	// Tick 3: reconcileVMStates sees GitHub online and promotes to Running.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 3: %v", err)
	}
	s.mu.Lock()
	state = s.vms[0].State
	s.mu.Unlock()
	if state != Running {
		t.Fatalf("expected VM Running once GitHub reports the runner online, got %v", state)
	}
}

func TestReconcileProvisioning_FailsAfterTimeoutIfGitHubNeverReportsOnline(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}
	rs := newFakeRunnerStatusChecker() // never online

	s := testScheduler(t, 1, targets, ds, prov, reg, withRunnerStatus(rs))

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)

	// Tick 2: reconcileVMStates resolves IP and launches runner.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}

	// Backdate AssignedAt past provisioningTimeout to simulate elapsed time
	// without a real sleep.
	s.mu.Lock()
	s.vms[0].AssignedAt = time.Now().Add(-provisioningTimeout - time.Second)
	s.mu.Unlock()

	// Tick 3: runner launched but never online and provisioning timed out.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 3: %v", err)
	}
	s.mu.Lock()
	state := s.vms[0].State
	s.mu.Unlock()
	if state != Failed {
		t.Fatalf("expected VM Failed after provisioningTimeout with runner never online, got %v", state)
	}
}

func TestReconcileRunning_DrainsAfterGitHubOfflineGracePeriod(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}
	rs := newFakeRunnerStatusChecker()

	s := testScheduler(t, 1, targets, ds, prov, reg, withRunnerStatus(rs))

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	s.mu.Lock()
	instanceName := s.vms[0].InstanceName
	s.mu.Unlock()
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)

	// Tick 2: reconcileVMStates resolves IP and launches runner.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}

	rs.setOnline(instanceName, true)

	// Tick 3: promotes to Running (tart alive, GitHub online).
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 3: %v", err)
	}
	s.mu.Lock()
	state := s.vms[0].State
	s.mu.Unlock()
	if state != Running {
		t.Fatalf("expected Running after Tick 3, got %v", state)
	}

	// Runner goes offline on GitHub while tart still reports the process alive.
	rs.setOnline(instanceName, false)

	// Tick 4: within grace period, VM should remain Running, not drained yet.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 4: %v", err)
	}
	s.mu.Lock()
	state = s.vms[0].State
	offlineSince := s.vms[0].GitHubOfflineSince
	s.mu.Unlock()
	if state != Running {
		t.Fatalf("expected VM to remain Running within grace period, got %v", state)
	}
	if offlineSince.IsZero() {
		t.Fatal("expected GitHubOfflineSince to be set once offline observed")
	}

	// Backdate the offline-since timestamp past the grace period to simulate
	// elapsed time without a real sleep.
	s.mu.Lock()
	s.vms[0].GitHubOfflineSince = time.Now().Add(-githubOfflineGrace - time.Second)
	s.mu.Unlock()

	// Tick 5: past grace period, VM should be drained back to Idle.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 5: %v", err)
	}
	s.mu.Lock()
	state = s.vms[0].State
	s.mu.Unlock()
	if state != Idle {
		t.Fatalf("expected VM drained to Idle after offline grace period elapsed, got %v", state)
	}
}

func TestReconcileRunning_RecoversFromTransientGitHubOfflineBlip(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}
	rs := newFakeRunnerStatusChecker()

	s := testScheduler(t, 1, targets, ds, prov, reg, withRunnerStatus(rs))

	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	s.mu.Lock()
	instanceName := s.vms[0].InstanceName
	s.mu.Unlock()
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)

	// Tick 2: reconcileVMStates resolves IP and launches runner.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}

	rs.setOnline(instanceName, true)

	// Tick 3: promotes to Running.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 3: %v", err)
	}

	rs.setOnline(instanceName, false)
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 4: %v", err)
	}
	s.mu.Lock()
	offlineSince := s.vms[0].GitHubOfflineSince
	s.mu.Unlock()
	if offlineSince.IsZero() {
		t.Fatal("expected GitHubOfflineSince set after offline observed")
	}

	rs.setOnline(instanceName, true)
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 5: %v", err)
	}
	s.mu.Lock()
	state := s.vms[0].State
	offlineSince = s.vms[0].GitHubOfflineSince
	s.mu.Unlock()
	if state != Running {
		t.Fatalf("expected VM to remain Running after recovering online, got %v", state)
	}
	if !offlineSince.IsZero() {
		t.Errorf("expected GitHubOfflineSince cleared after recovering online, got %v", offlineSince)
	}
}

func TestReconcileRunning_DrainsIdleRunnerAfterGracePeriod(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}
	rs := newFakeRunnerStatusChecker()

	s := testScheduler(t, 1, targets, ds, prov, reg, withRunnerStatus(rs))

	// Tick 1: provisions.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	s.mu.Lock()
	instanceName := s.vms[0].InstanceName
	s.mu.Unlock()
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)

	// Tick 2: resolves IP and launches runner.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}

	// Tick 3: runner comes online but idle (not busy).
	rs.setOnlineAndBusy(instanceName, true, false)
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 3: %v", err)
	}
	s.mu.Lock()
	state := s.vms[0].State
	s.mu.Unlock()
	if state != Running {
		t.Fatalf("expected Running after Tick 3, got %v", state)
	}

	// Tick 4: still idle, but not yet past idle grace — should stay Running.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 4: %v", err)
	}
	s.mu.Lock()
	state = s.vms[0].State
	s.mu.Unlock()
	if state != Running {
		t.Fatalf("expected Running within idle grace, got %v", state)
	}

	// Backdate RunnerIdleSince to simulate elapsed idle time.
	s.mu.Lock()
	s.vms[0].RunnerIdleSince = time.Now().Add(-runnerIdleGrace - time.Second)
	s.mu.Unlock()

	// Tick 5: idle grace exceeded — should drain to Idle.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 5: %v", err)
	}
	s.mu.Lock()
	state = s.vms[0].State
	s.mu.Unlock()
	if state != Idle {
		t.Fatalf("expected VM drained to Idle after idle grace elapsed, got %v", state)
	}
}

func TestReconcileRunning_BusyRunnerNeverDrained(t *testing.T) {
	targets := []TargetRef{{Owner: "acme", Repo: "repo1"}}
	ds := &fakeDemandSource{}
	ds.setQueued(map[string]int{"acme/repo1": 1}, targets)
	prov := newFakeProvisioner()
	reg := &fakeRegistrar{}
	rs := newFakeRunnerStatusChecker()

	s := testScheduler(t, 1, targets, ds, prov, reg, withRunnerStatus(rs))

	// Tick 1: provisions.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	s.mu.Lock()
	instanceName := s.vms[0].InstanceName
	s.mu.Unlock()
	ds.setQueued(map[string]int{"acme/repo1": 0}, targets)

	// Tick 2: resolves IP and launches runner.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}

	// Tick 3: runner comes online and busy.
	rs.setOnlineAndBusy(instanceName, true, true)
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 3: %v", err)
	}

	// Backdate RunnerIdleSince (which should never be set for busy runners).
	s.mu.Lock()
	s.vms[0].RunnerIdleSince = time.Now().Add(-runnerIdleGrace - time.Hour)
	s.mu.Unlock()

	// Tick 4: runner is busy — should NOT drain even if we backdated idle timer.
	if err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick 4: %v", err)
	}
	s.mu.Lock()
	state := s.vms[0].State
	s.mu.Unlock()
	if state != Running {
		t.Fatalf("expected VM to stay Running while busy, got %v", state)
	}
}
