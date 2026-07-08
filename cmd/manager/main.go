// Command mac-runners-manager manages a small pool of Tart VMs, cycling them
// between GitHub repos as ephemeral Actions runners based on queued-job
// demand.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	ghinternal "github.com/barelyhuman/mac-runners-manager/internal/github"
	"github.com/barelyhuman/mac-runners-manager/internal/jsconfig"
	"github.com/barelyhuman/mac-runners-manager/internal/scheduler"
	"github.com/barelyhuman/mac-runners-manager/internal/sshdebug"
	"github.com/barelyhuman/mac-runners-manager/internal/tart"
	"github.com/barelyhuman/mac-runners-manager/internal/vmprovisioner"
)

const sshReadyWait = 2 * time.Minute

// defaultDiagCommands are read-only checks likely to work against any
// macOS golden image, regardless of exactly how its boot-time agent is
// wired up: they look for common runner artifact/log locations and any
// launchd job or process whose name mentions "runner" or "actions".
var defaultDiagCommands = []string{
	`launchctl list | grep -i -E 'runner|actions' || echo "(no matching launchd jobs)"`,
	`ps aux | grep -i -E '[Rr]unner\.Listener|actions-runner' || echo "(no matching processes)"`,
	`ls -la ~/action-runner 2>/dev/null || echo "(no ~/action-runner directory)"`,
	`tail -n 80 ~/action-runner/_diag/*.log 2>/dev/null || echo "(no runner diag logs found)"`,
	`ls -la /Library/LaunchDaemons /Library/LaunchAgents 2>/dev/null | grep -i -E 'runner|actions' || echo "(no matching launchd plists)"`,
}

func main() {
	configPath := flag.String("config", "config.js", "path to the JS config file")
	tartBinary := flag.String("tart-binary", "tart", "path to the tart CLI binary")
	baseImage := flag.String("base-image", "", "golden Tart image to clone for each ephemeral VM")
	stateDir := flag.String("state-dir", "/tmp/mac-runners-manager", "scratch directory for per-VM state and logs")
	netBridged := flag.String("net-bridged", "", "host network interface (e.g. en0) to bridge VMs onto so the host can SSH into them directly; empty uses tart's default shared/NAT networking")
	verbose := flag.Bool("verbose", false, "enable debug logging of demand polling, allocation, and tart CLI calls")

	sshDebugInstance := flag.String("ssh-debug", "", "instance name of a booted VM to inspect over SSH, then exit (does not run the scheduler)")
	sshUser := flag.String("ssh-user", "admin", "SSH user for connecting to VM guests")
	sshPassword := flag.String("ssh-password", "", "SSH password for VM guests (or set SSH_DEBUG_PASSWORD)")
	sshKeyPath := flag.String("ssh-key", "", "path to a PEM-encoded private key for VM guest SSH")
	sshWaitSeconds := flag.Int("ssh-wait", 30, "seconds to wait for -ssh-debug's target VM to report an IP")
	tailRunnerLogs := flag.Bool("tail-runner-logs", false, "stream each runner's diagnostic logs to the agent's stdout")
	vmMemory := flag.Int("vm-memory", 0, "VM memory size in megabytes (e.g. 4096 = 4GB). Zero leaves the base image's default.")

	var diagCmds stringList
	flag.Var(&diagCmds, "diag-cmd", "diagnostic command to run over SSH for -ssh-debug (repeatable; defaults to a built-in set if omitted)")
	flag.Parse()

	debugLog := log.New(io.Discard, "", 0)
	if *verbose {
		debugLog = log.New(os.Stderr, "[debug] ", log.LstdFlags)
	}

	if *sshDebugInstance != "" {
		if err := runSSHDebug(*tartBinary, *netBridged, *sshDebugInstance, *sshUser, *sshPassword, *sshKeyPath, *sshWaitSeconds, diagCmds, debugLog); err != nil {
			log.Fatalf("mac-runners-manager: ssh-debug failed: %v", err)
		}
		return
	}

	if *baseImage == "" {
		log.Fatal("mac-runners-manager: -base-image is required")
	}

	cfg, err := jsconfig.Load(*configPath)
	if err != nil {
		log.Fatalf("mac-runners-manager: failed to load config: %v", err)
	}

	if err := os.MkdirAll(*stateDir, 0o700); err != nil {
		log.Fatalf("mac-runners-manager: failed to create state dir: %v", err)
	}

	// Resolve SSH credentials: CLI flags override JS config values.
	sshCreds := resolveSSHCredentials(*sshUser, *sshPassword, *sshKeyPath, cfg.SSHCredentials)
	sshClient, err := buildSSHClient(sshCreds)
	if err != nil {
		log.Fatalf("mac-runners-manager: failed to build SSH client: %v", err)
	}

	tartClient := tart.NewClient(*tartBinary, *stateDir, debugLog).WithNetBridged(*netBridged)
	demandSource := ghinternal.NewPollingDemandSource(cfg.Auth, debugLog)
	registrar := ghinternal.NewJITRegistrar(cfg.Auth, debugLog)
	runnerStatus := ghinternal.NewRunnerStatusChecker(cfg.Auth, debugLog)
	staleCleaner := ghinternal.NewStaleRunnerCleaner(cfg.Auth, debugLog)
	guestRunner := vmprovisioner.New(sshClient, "", debugLog)

	sched := scheduler.New(scheduler.Config{
		Demand:        demandSource,
		Provisioner:   tartClient,
		Registrar:     registrar,
		RunnerStatus:  runnerStatus,
		RunnerCleaner: staleCleaner,
		GuestRunner:   guestRunner,
		Targets:       cfg.Targets,
		Priority:      cfg.Priority,
		PoolSize:      cfg.PoolSize,
		TickEvery:     cfg.TickInterval,
		BaseImage:     *baseImage,
		RunnerVersion: cfg.RunnerVersion,
		Debug:         debugLog,
		ForceSpawn:    cfg.ForceSpawn,
		VMMemoryMB:    resolveVMMemory(*vmMemory, cfg.VMMemoryMB),
		TailLogs:      *tailRunnerLogs,
	})

	log.Printf("mac-runners-manager: starting with pool size %d, tick interval %s, %d target(s), runner version %q",
		cfg.PoolSize, cfg.TickInterval, len(cfg.Targets), cfg.RunnerVersion)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := sched.Run(ctx); err != nil {
		log.Fatalf("mac-runners-manager: scheduler exited with error: %v", err)
	}
	log.Println("mac-runners-manager: shutting down")
}

// resolveVMMemory merges CLI flag with JS config value. CLI flag takes
// precedence when explicitly set (non-zero).
func resolveVMMemory(cliMB, cfgMB int) int {
	if cliMB > 0 {
		return cliMB
	}
	return cfgMB
}

// resolveSSHCredentials merges CLI flag values with JS config values. CLI
// flags take precedence for any field they explicitly set.
func resolveSSHCredentials(cliUser, cliPassword, cliKeyPath string, cfg jsconfig.SSHCredentials) sshdebug.Config {
	creds := sshdebug.Config{
		User:     cfg.User,
		Password: cfg.Password,
	}
	if cliUser != "" && cliUser != "admin" {
		creds.User = cliUser
	} else if creds.User == "" {
		creds.User = "admin"
	}
	if cliPassword != "" {
		creds.Password = cliPassword
	}
	if cliKeyPath != "" {
		creds.Key = mustReadKey(cliKeyPath)
	} else if cfg.KeyPath != "" {
		creds.Key = mustReadKey(cfg.KeyPath)
	}
	if creds.Password == "" {
		creds.Password = os.Getenv("SSH_DEBUG_PASSWORD")
	}
	return creds
}

func mustReadKey(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("mac-runners-manager: read SSH key %s: %v", path, err)
	}
	return b
}

func buildSSHClient(creds sshdebug.Config) (*sshdebug.Client, error) {
	if creds.Password == "" && len(creds.Key) == 0 {
		return nil, fmt.Errorf("SSH credentials incomplete: provide -ssh-password (or SSH_DEBUG_PASSWORD) or -ssh-key")
	}
	return sshdebug.New(creds)
}

// runSSHDebug resolves the named VM's IP via `tart ip`, connects over SSH,
// and runs a set of read-only diagnostic commands, printing output for each.
// One-shot: it does not start the scheduler.
func runSSHDebug(tartBinary, netBridged, instanceName, user, password, keyPath string, waitSeconds int, diagCmds []string, debugLog *log.Logger) error {
	if password == "" {
		password = os.Getenv("SSH_DEBUG_PASSWORD")
	}

	var key []byte
	if keyPath != "" {
		b, err := os.ReadFile(keyPath)
		if err != nil {
			return fmt.Errorf("read -ssh-key %s: %w", keyPath, err)
		}
		key = b
	}
	if len(key) == 0 && password == "" {
		return fmt.Errorf("either -ssh-password (or SSH_DEBUG_PASSWORD) or -ssh-key is required")
	}

	commands := diagCmds
	if len(commands) == 0 {
		commands = defaultDiagCommands
	}

	tartClient := tart.NewClient(tartBinary, os.TempDir(), debugLog).WithNetBridged(netBridged)
	ctx := context.Background()

	log.Printf("mac-runners-manager: resolving IP for %s (waiting up to %ds)", instanceName, waitSeconds)
	ip, err := tartClient.IP(ctx, instanceName, waitSeconds)
	if err != nil {
		return fmt.Errorf("resolve IP for %s: %w", instanceName, err)
	}
	log.Printf("mac-runners-manager: %s resolved to %s", instanceName, ip)

	client, err := sshdebug.New(sshdebug.Config{
		User:     user,
		Password: password,
		Key:      key,
	})
	if err != nil {
		return err
	}

	log.Printf("mac-runners-manager: waiting up to %s for SSH on %s (%s)", sshReadyWait, instanceName, ip)
	results, err := client.RunWhenReady(ip, commands, sshReadyWait)
	if err != nil {
		return fmt.Errorf("ssh to %s (%s): %w", instanceName, ip, err)
	}

	for _, r := range results {
		fmt.Printf("\n=== %s ===\n", r.Command)
		if r.Err != nil {
			fmt.Printf("(command error: %v)\n", r.Err)
		}
		fmt.Println(r.Output)
	}
	return nil
}

// stringList collects repeated -diag-cmd flag values.
type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ", ")
}

func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}
