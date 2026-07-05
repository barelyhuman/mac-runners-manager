// Package tart wraps the `tart` CLI (github.com/cirruslabs/tart) to manage
// ephemeral VM clones used as GitHub Actions runners.
package tart

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/usebruno/mac-action-agent/internal/scheduler"
)

// execFunc runs a command and returns its combined stdout/stderr output.
// Swappable in tests so no real `tart` binary is required.
type execFunc func(ctx context.Context, name string, args ...string) ([]byte, error)

// Client shells out to the `tart` binary to manage VM lifecycle.
type Client struct {
	binary     string
	run        execFunc
	baseDir    string // scratch dir for per-VM boot payload mounts
	debug      *log.Logger
	netBridged string // host interface name for `tart run --net-bridged`; empty disables it
}

// NewClient constructs a tart CLI client. binary is the path to the `tart`
// executable (e.g. "tart", resolved via PATH). baseDir is a scratch directory
// used to stage boot payload files mounted into guests. debug receives
// verbose tracing of every tart invocation and its output; pass nil to
// disable it.
func NewClient(binary, baseDir string, debug *log.Logger) *Client {
	if debug == nil {
		debug = log.New(io.Discard, "", 0)
	}
	return &Client{
		binary:  binary,
		baseDir: baseDir,
		run:     defaultExec,
		debug:   debug,
	}
}

// WithNetBridged configures every subsequent Boot call to run VMs with
// `--net-bridged=<interface>` instead of Tart's default shared/NAT
// networking. This is required for the host to be able to SSH into the VM
// directly by IP (the golden image must also have Remote Login enabled).
// An empty interface name leaves the default networking mode untouched.
func (c *Client) WithNetBridged(iface string) *Client {
	c.netBridged = iface
	return c
}

func defaultExec(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err != nil {
		return out.Bytes(), fmt.Errorf("%s %v: %w: %s", name, args, err, out.String())
	}
	return out.Bytes(), nil
}

func (c *Client) tart(ctx context.Context, args ...string) ([]byte, error) {
	c.debug.Printf("tart: running %s %v", c.binary, args)
	out, err := c.run(ctx, c.binary, args...)
	if err != nil {
		c.debug.Printf("tart: %s %v failed: %v", c.binary, args, err)
	} else {
		c.debug.Printf("tart: %s %v output: %s", c.binary, args, bytes.TrimSpace(out))
	}
	return out, err
}

// Clone creates a new ephemeral VM instance from a base image.
func (c *Client) Clone(ctx context.Context, baseImage, instanceName string) error {
	_, err := c.tart(ctx, "clone", baseImage, instanceName)
	return err
}

// payloadDir returns (and ensures exists) the scratch directory mounted into
// a given VM instance to deliver its JIT registration config at boot.
func (c *Client) payloadDir(instanceName string) (string, error) {
	dir := filepath.Join(c.baseDir, instanceName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create payload dir: %w", err)
	}
	return dir, nil
}

// bootArgs builds the `tart run` argument list for booting instanceName with
// its payload directory mounted, applying bridged networking if configured.
func (c *Client) bootArgs(instanceName, payloadDir string) []string {
	args := []string{"run", instanceName, "--no-graphics", "--dir", "runner:" + payloadDir}
	if c.netBridged != "" {
		args = append(args, "--net-bridged="+c.netBridged)
	}
	return args
}

// Boot starts the VM headless, mounting a directory containing the JIT
// registration payload that the golden image's boot-time agent reads.
func (c *Client) Boot(ctx context.Context, instanceName string, payload scheduler.BootPayload) error {
	dir, err := c.payloadDir(instanceName)
	if err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join(dir, "jitconfig"), []byte(payload.JITConfig), 0o600); err != nil {
		return fmt.Errorf("write jitconfig payload: %w", err)
	}

	args := c.bootArgs(instanceName, dir)

	c.debug.Printf("tart: starting detached %s %v", c.binary, args)
	cmd := exec.CommandContext(context.Background(), c.binary, args...)
	setDetached(cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("tart run %s: %w", instanceName, err)
	}
	// Detach: the scheduler tracks VM state via `tart list`/GitHub API on
	// later ticks, not by waiting on this process.
	go func() {
		err := cmd.Wait()
		if err != nil {
			c.debug.Printf("tart: detached %s run for %s exited with error: %v (if this happened immediately, the VM likely never came up)", c.binary, instanceName, err)
		} else {
			c.debug.Printf("tart: detached %s run for %s exited cleanly", c.binary, instanceName)
		}
	}()

	return nil
}

// IsRunning reports whether the named instance is currently running per
// `tart list`.
func (c *Client) IsRunning(ctx context.Context, instanceName string) (bool, error) {
	names, err := c.List(ctx)
	if err != nil {
		return false, err
	}
	for _, n := range names {
		if n == instanceName {
			return true, nil
		}
	}
	return false, nil
}

// IP resolves the instance's IP address via `tart ip`, waiting up to
// waitSeconds for a booting VM to acquire one.
func (c *Client) IP(ctx context.Context, instanceName string, waitSeconds int) (string, error) {
	out, err := c.tart(ctx, "ip", instanceName, "--wait", fmt.Sprintf("%d", waitSeconds))
	if err != nil {
		return "", fmt.Errorf("tart ip %s: %w", instanceName, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// Stop gracefully stops the VM, forcing it if it doesn't respond.
func (c *Client) Stop(ctx context.Context, instanceName string) error {
	if _, err := c.tart(ctx, "stop", instanceName); err != nil {
		_, forceErr := c.tart(ctx, "stop", "--force", instanceName)
		if forceErr != nil {
			return fmt.Errorf("stop %s (graceful: %v): %w", instanceName, err, forceErr)
		}
	}
	return nil
}

// Delete removes the ephemeral clone's disk to reclaim space.
func (c *Client) Delete(ctx context.Context, instanceName string) error {
	_, err := c.tart(ctx, "delete", instanceName)
	if err == nil {
		os.RemoveAll(filepath.Join(c.baseDir, instanceName))
	}
	return err
}

type tartListEntry struct {
	Name    string `json:"Name"`
	Running bool   `json:"Running"`
}

// List enumerates currently-running instances.
func (c *Client) List(ctx context.Context) ([]string, error) {
	out, err := c.tart(ctx, "list", "--format", "json")
	if err != nil {
		return nil, err
	}
	var entries []tartListEntry
	if err := json.Unmarshal(out, &entries); err != nil {
		return nil, fmt.Errorf("parse tart list output: %w", err)
	}
	var running []string
	for _, e := range entries {
		if e.Running {
			running = append(running, e.Name)
		}
	}
	return running, nil
}
