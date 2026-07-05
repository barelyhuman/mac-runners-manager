package tart

import (
	"os/exec"
	"syscall"
)

// setDetached configures cmd to run in its own process group so the agent
// can supervise it independently of its own lifecycle (e.g. surviving a
// SIGTERM to the agent without killing in-flight VMs).
func setDetached(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
