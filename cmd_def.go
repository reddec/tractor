//+build !linux

package tractor

import (
	"os/exec"
	"syscall"
)

func setupCmdFlags(cmd *exec.Cmd) {

}

func killCmd(cmd *exec.Cmd) error {
	return cmd.Process.Kill()
}

func sendSig(cmd *exec.Cmd, sig syscall.Signal) error {
	return cmd.Process.Signal(sig)
}
