package jsconfig

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
)

const defaultExecTimeout = 60 * time.Second

var (
	execTimeoutMu sync.RWMutex
	execTimeout   = defaultExecTimeout
)

// setExecTimeout updates the timeout used by the host exec() function.
// A value of zero resets to the defaultExecTimeout.
func setExecTimeout(seconds int) {
	execTimeoutMu.Lock()
	defer execTimeoutMu.Unlock()
	if seconds > 0 {
		execTimeout = time.Duration(seconds) * time.Second
	} else {
		execTimeout = defaultExecTimeout
	}
}

// getExecTimeout returns the current timeout for the host exec() function.
func getExecTimeout() time.Duration {
	execTimeoutMu.RLock()
	defer execTimeoutMu.RUnlock()
	return execTimeout
}

// registerHostFunctions exposes the small set of globals the config script
// may call: env() for reading environment variables, exec() for shelling out
// (e.g. to a keychain/secrets CLI), and log() for routing messages through
// the Go logger. The config file is user-authored, so no sandboxing beyond
// this limited surface (no fs/net/process access outside these functions) is
// needed — it's not untrusted input.
func registerHostFunctions(vm *goja.Runtime) {
	vm.Set("env", hostEnv)
	vm.Set("exec", hostExec)
	vm.Set("log", hostLog)
}

func hostEnv(name string) string {
	return os.Getenv(name)
}

func hostExec(cmd string, args ...string) (string, error) {
	timeout := getExecTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	c := exec.CommandContext(ctx, cmd, args...)
	var out, stderr bytes.Buffer
	c.Stdout = &out
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		return "", fmt.Errorf("exec(%q, %v) failed: %v: %s", cmd, args, err, stderr.String())
	}
	return strings.TrimRight(out.String(), "\n"), nil
}

func hostLog(args ...interface{}) {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = fmt.Sprint(a)
	}
	fmt.Println("[jsconfig]", strings.Join(parts, " "))
}
