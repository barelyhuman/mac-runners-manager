package sshdebug

import (
	"errors"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func TestRunWhenReady_SucceedsImmediately(t *testing.T) {
	client := newTestClient(t)
	client.dial = func(network, addr string, config *ssh.ClientConfig) (sshConn, error) {
		if network != "tcp" {
			t.Fatalf("network = %q, want tcp", network)
		}
		if addr != "192.0.2.10:22" {
			t.Fatalf("addr = %q, want 192.0.2.10:22", addr)
		}
		return &fakeConn{}, nil
	}

	results, err := client.RunWhenReady("192.0.2.10", []string{"echo ready"}, time.Minute)
	if err != nil {
		t.Fatalf("RunWhenReady: %v", err)
	}
	if len(results) != 1 || results[0].Output != "ok: echo ready" {
		t.Fatalf("results = %+v, want one successful command", results)
	}
}

func TestRunWhenReady_RetriesUntilSSHAcceptsConnections(t *testing.T) {
	client := newTestClient(t)
	dialErr := errors.New("connection refused")
	attempts := 0
	sleeps := 0

	client.dial = func(network, addr string, config *ssh.ClientConfig) (sshConn, error) {
		attempts++
		if attempts < 3 {
			return nil, dialErr
		}
		return &fakeConn{}, nil
	}
	client.sleep = func(time.Duration) {
		sleeps++
	}

	_, err := client.RunWhenReady("192.0.2.10", nil, time.Minute)
	if err != nil {
		t.Fatalf("RunWhenReady: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if sleeps != 2 {
		t.Fatalf("sleeps = %d, want 2", sleeps)
	}
}

func TestRunWhenReady_ReturnsLastDialErrorWhenNotReady(t *testing.T) {
	client := newTestClient(t)
	dialErr := errors.New("connection refused")
	client.dial = func(network, addr string, config *ssh.ClientConfig) (sshConn, error) {
		return nil, dialErr
	}

	_, err := client.RunWhenReady("192.0.2.10", nil, 0)
	if err == nil {
		t.Fatal("expected readiness error")
	}
	if !strings.Contains(err.Error(), "within 0s") {
		t.Fatalf("error = %q, want retry budget", err)
	}
	if !errors.Is(err, dialErr) {
		t.Fatalf("error = %v, want wrapping %v", err, dialErr)
	}
}

func newTestClient(t *testing.T) *Client {
	t.Helper()
	client, err := New(Config{User: "admin", Password: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	client.sleep = func(time.Duration) {}
	return client
}

type fakeConn struct{}

func (c *fakeConn) Close() error {
	return nil
}

func (c *fakeConn) NewSession() (sshSession, error) {
	return fakeSession{}, nil
}

type fakeSession struct{}

func (s fakeSession) Close() error {
	return nil
}

func (s fakeSession) CombinedOutput(cmd string) ([]byte, error) {
	return []byte("ok: " + cmd + "\n"), nil
}
