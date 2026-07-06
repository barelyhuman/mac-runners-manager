// Package vmprovisioner orchestrates the installation, configuration, and
// lifecycle of a GitHub Actions runner inside a Tart VM's guest OS over SSH.
package vmprovisioner

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/barelyhuman/mac-runners-manager/internal/sshdebug"
)

const (
	defaultInstallDir = "~/action-runner"
	jitConfigFile     = ".jitconfig"
	versionFile       = ".runner-version"
)

// GuestProvisioner runs commands inside a VM over SSH to check for,
// download, extract, configure, and start the GitHub Actions runner.
type GuestProvisioner struct {
	ssh     *sshdebug.Client
	debug   *log.Logger
	install string // guest install directory
}

// New constructs a GuestProvisioner from an existing SSH client. installDir
// defaults to ~/action-runner if empty.
func New(sshClient *sshdebug.Client, installDir string, debug *log.Logger) *GuestProvisioner {
	if installDir == "" {
		installDir = defaultInstallDir
	}
	if debug == nil {
		debug = log.New(io.Discard, "", 0)
	}
	return &GuestProvisioner{
		ssh:     sshClient,
		install: installDir,
		debug:   debug,
	}
}

// IsInstalled checks whether ~/action-runner/run.sh exists inside the guest.
func (g *GuestProvisioner) IsInstalled(ctx context.Context, ip string) (bool, error) {
	g.debug.Printf("vmprovisioner: checking if runner is installed on %s", ip)
	results, err := g.ssh.Run(ip, []string{
		fmt.Sprintf("test -f %s/run.sh && echo 'installed' || echo 'missing'", g.install),
	})
	if err != nil {
		return false, fmt.Errorf("ssh check for run.sh: %w", err)
	}
	if len(results) == 0 {
		return false, nil
	}
	installed := strings.TrimSpace(results[0].Output) == "installed"
	g.debug.Printf("vmprovisioner: runner installed = %v on %s", installed, ip)
	return installed, nil
}

// Version returns the version of the installed runner, or empty string if
// not installed / version unknown.
func (g *GuestProvisioner) Version(ctx context.Context, ip string) (string, error) {
	results, err := g.ssh.Run(ip, []string{
		fmt.Sprintf("cat %s/%s 2>/dev/null || echo ''", g.install, versionFile),
	})
	if err != nil {
		return "", fmt.Errorf("ssh read version file: %w", err)
	}
	if len(results) == 0 {
		return "", nil
	}
	return strings.TrimSpace(results[0].Output), nil
}

// Install downloads the GitHub Actions runner tarball for the given version
// (or latest if empty), extracts it to the install directory, and writes a
// small version marker file so Version() can be checked later.
func (g *GuestProvisioner) Install(ctx context.Context, ip, versionTag string) error {
	url, err := runnerDownloadURL(versionTag)
	if err != nil {
		return fmt.Errorf("resolve runner download URL: %w", err)
	}
	g.debug.Printf("vmprovisioner: downloading runner from %s on %s", url, ip)

	cmds := []string{
		// Ensure install directory exists
		fmt.Sprintf("mkdir -p %s", g.install),
		// Write version marker
		fmt.Sprintf("echo '%s' > %s/%s", versionTag, g.install, versionFile),
		// Download
		fmt.Sprintf("curl -fsSL -o /tmp/actions-runner.tar.gz '%s'", url),
		// Extract
		fmt.Sprintf("tar -xzf /tmp/actions-runner.tar.gz -C %s", g.install),
		// Cleanup
		"rm -f /tmp/actions-runner.tar.gz",
	}

	results, err := g.ssh.Run(ip, cmds)
	if err != nil {
		return fmt.Errorf("ssh install runner: %w", err)
	}

	for _, r := range results {
		if r.Err != nil {
			return fmt.Errorf("install command %q failed: %v (output: %s)", r.Command, r.Err, r.Output)
		}
	}

	g.debug.Printf("vmprovisioner: runner installed successfully on %s", ip)
	return nil
}

// WriteJITConfig writes the base64-encoded JIT config to a temporary file
// inside the guest and returns the guest-side absolute path to that file.
func (g *GuestProvisioner) WriteJITConfig(ctx context.Context, ip, jitConfig string) (string, error) {
	path := fmt.Sprintf("%s/%s", g.install, jitConfigFile)
	g.debug.Printf("vmprovisioner: writing JIT config to %s on %s", path, ip)

	// Use a heredoc to avoid issues with special characters in the base64 string.
	cmd := fmt.Sprintf("cat > %s << 'JITCONFIG_EOF'\n%s\nJITCONFIG_EOF", path, jitConfig)
	results, err := g.ssh.Run(ip, []string{cmd})
	if err != nil {
		return "", fmt.Errorf("ssh write jitconfig: %w", err)
	}
	if len(results) > 0 && results[0].Err != nil {
		return "", fmt.Errorf("write jitconfig failed: %v (output: %s)", results[0].Err, results[0].Output)
	}
	return path, nil
}

// StartRunner launches ./run.sh --jitconfig <path> inside the guest via nohup
// so it survives the SSH session closing.
func (g *GuestProvisioner) StartRunner(ctx context.Context, ip, jitConfigPath string) error {
	g.debug.Printf("vmprovisioner: starting runner on %s with jitconfig %s", ip, jitConfigPath)

	cmd := fmt.Sprintf(
		"cd %s && nohup ./run.sh --jitconfig \"$(cat %s)\" > ./runner.log 2>&1 &",
		g.install, jitConfigPath,
	)
	results, err := g.ssh.Run(ip, []string{cmd})
	if err != nil {
		return fmt.Errorf("ssh start runner: %w", err)
	}
	if len(results) > 0 && results[0].Err != nil {
		return fmt.Errorf("start runner failed: %v (output: %s)", results[0].Err, results[0].Output)
	}

	g.debug.Printf("vmprovisioner: runner started on %s", ip)
	return nil
}

// KillRunner terminates any existing run.sh process inside the guest.
func (g *GuestProvisioner) KillRunner(ctx context.Context, ip string) error {
	g.debug.Printf("vmprovisioner: killing any existing runner on %s", ip)
	results, err := g.ssh.Run(ip, []string{
		"pkill -f 'run.sh' || true",
	})
	if err != nil {
		return fmt.Errorf("ssh kill runner: %w", err)
	}
	_ = results // pkill exits 1 if nothing matched, which is fine
	return nil
}

// TailLogs returns a ReadCloser that streams the runner diagnostic logs.
// The caller must Close() the returned reader when done.
func (g *GuestProvisioner) TailLogs(ctx context.Context, ip string) (io.ReadCloser, error) {
	// We open a dedicated SSH session and run tail -f so the stream survives
	// as long as the connection is open.  The caller can Close() the pipe
	// reader to stop tailing.
	g.debug.Printf("vmprovisioner: starting log tail on %s", ip)
	pr, pw := io.Pipe()

	// run tail in a background goroutine
	go func() {
		defer pw.Close()
		cmd := fmt.Sprintf("tail -f %s/_diag/*.log 2>/dev/null || echo '(no diag logs yet)'", g.install)
		results, err := g.ssh.Run(ip, []string{cmd})
		if err != nil {
			g.debug.Printf("vmprovisioner: log tail error on %s: %v", ip, err)
			return
		}
		for _, r := range results {
			if r.Err != nil {
				g.debug.Printf("vmprovisioner: log tail command error on %s: %v", ip, r.Err)
				return
			}
			io.WriteString(pw, r.Output+"\n")
		}
	}()

	return pr, nil
}
