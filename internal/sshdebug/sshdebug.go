// Package sshdebug provides a one-shot SSH client for inspecting a booted
// Tart VM's guest state (launchd jobs, runner process, logs) when the
// golden image's boot-time runner registration isn't behaving as expected.
//
// This is a manual diagnostic tool, not something the scheduler's
// reconciliation loop depends on: the golden image is built and owned
// outside this repo, so its SSH availability, credentials, and guest paths
// are not guarantees this codebase can rely on for normal operation.
package sshdebug

import (
	"fmt"
	"net"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

// Config describes how to reach and authenticate to a booted VM's guest OS.
type Config struct {
	User        string
	Password    string // optional; used if Key is empty
	Key         []byte // optional PEM-encoded private key; takes precedence over Password
	DialTimeout time.Duration
}

// Client runs read-only diagnostic commands over SSH against a single host.
type Client struct {
	cfg   Config
	dial  dialFunc
	sleep func(time.Duration)
}

type dialFunc func(network, addr string, config *ssh.ClientConfig) (sshConn, error)

type sshConn interface {
	Close() error
	NewSession() (sshSession, error)
}

type sshSession interface {
	Close() error
	CombinedOutput(cmd string) ([]byte, error)
}

type realSSHConn struct {
	*ssh.Client
}

func (c realSSHConn) NewSession() (sshSession, error) {
	return c.Client.NewSession()
}

// New constructs a Client from the given auth config.
func New(cfg Config) (*Client, error) {
	if cfg.User == "" {
		return nil, fmt.Errorf("sshdebug: User is required")
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	return &Client{
		cfg: cfg,
		dial: func(network, addr string, config *ssh.ClientConfig) (sshConn, error) {
			conn, err := ssh.Dial(network, addr, config)
			if err != nil {
				return nil, err
			}
			return realSSHConn{Client: conn}, nil
		},
		sleep: time.Sleep,
	}, nil
}

func (c *Client) authMethods() ([]ssh.AuthMethod, error) {
	if len(c.cfg.Key) > 0 {
		signer, err := ssh.ParsePrivateKey(c.cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		return []ssh.AuthMethod{ssh.PublicKeys(signer)}, nil
	}
	if c.cfg.Password != "" {
		return []ssh.AuthMethod{ssh.Password(c.cfg.Password)}, nil
	}
	return nil, fmt.Errorf("sshdebug: either Key or Password must be set")
}

// Result is the outcome of running one diagnostic command.
type Result struct {
	Command string
	Output  string
	Err     error
}

// Run connects to host:22 and executes each command in order, returning one
// Result per command. It stops and returns early only on a connection
// failure; individual command failures are captured per-Result so the rest
// still run (e.g. `tail` on a log path that doesn't exist on this image
// shouldn't prevent `launchctl list` from being tried).
//
// Host key verification is intentionally skipped: these are ephemeral,
// self-managed golden-image VMs on a local vmnet bridge, not
// internet-facing hosts, so pinning/verifying a host key adds no real
// security value here and would just add setup friction for a debug tool.
func (c *Client) Run(host string, commands []string) ([]Result, error) {
	conn, err := c.dialHost(host)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return c.runCommands(conn, commands), nil
}

// RunWhenReady retries SSH connection setup until either the guest accepts a
// connection or waitFor elapses, then runs diagnostics once.
func (c *Client) RunWhenReady(host string, commands []string, waitFor time.Duration) ([]Result, error) {
	deadline := time.Now().Add(waitFor)
	var lastErr error

	for {
		conn, err := c.dialHost(host)
		if err == nil {
			defer conn.Close()
			return c.runCommands(conn, commands), nil
		}

		lastErr = err
		if waitFor <= 0 || !time.Now().Before(deadline) {
			return nil, fmt.Errorf("ssh did not become ready on %s within %s: %w", host, waitFor, lastErr)
		}

		c.sleep(3 * time.Second)
	}
}

func (c *Client) dialHost(host string) (sshConn, error) {
	auth, err := c.authMethods()
	if err != nil {
		return nil, err
	}

	clientCfg := &ssh.ClientConfig{
		User:            c.cfg.User,
		Auth:            auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         c.cfg.DialTimeout,
	}

	addr := net.JoinHostPort(host, "22")
	conn, err := c.dial("tcp", addr, clientCfg)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}
	return conn, nil
}

func (c *Client) runCommands(conn sshConn, commands []string) []Result {
	results := make([]Result, 0, len(commands))
	for _, cmd := range commands {
		results = append(results, c.runOne(conn, cmd))
	}
	return results
}

func (c *Client) runOne(conn sshConn, cmd string) Result {
	session, err := conn.NewSession()
	if err != nil {
		return Result{Command: cmd, Err: fmt.Errorf("new session: %w", err)}
	}
	defer session.Close()

	out, err := session.CombinedOutput(cmd)
	return Result{Command: cmd, Output: strings.TrimRight(string(out), "\n"), Err: err}
}
