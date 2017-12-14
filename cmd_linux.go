//+build linux

package tractor

import (
	"os/exec"
	"syscall"
)

func setupCmdFlags(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
		Setpgid:   true,
	}
}

func killCmd(cmd *exec.Cmd) error {
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		return err
	}
	err = syscall.Kill(-pgid, syscall.SIGKILL)
	if err != nil {
		// fallback
		err = cmd.Process.Kill()
	}
	return err
}

func sendSig(cmd *exec.Cmd, sig syscall.Signal) error {
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		return err
	}
	err = syscall.Kill(-pgid, sig)
	if err != nil {
		// fallback
		err = cmd.Process.Signal(sig)
	}
	return err
}
