package tractor

import (
	"os/exec"
	"context"
	"time"
	"syscall"
)

// Own bike (instead of CreateCmdCtx) due to specific signals on different platform
func runWithContext(cmd *exec.Cmd, ctx context.Context, gracefulTimeout time.Duration) error {
	appRun := make(chan error, 1)
	go func() {
		appRun <- cmd.Run()
	}()

	var err error
	select {
	case <-ctx.Done():
		sendSig(cmd, syscall.SIGINT)
		select {
		case <-time.After(gracefulTimeout):
			killCmd(cmd)
			err = <-appRun
		case err = <-appRun:
		}
	case err = <-appRun:

	}
	return err
}
