package handlers

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

type ShellArgs struct {
	Command    string `json:"command"`
	TimeoutSec int    `json:"timeout_sec,omitempty"`
}

type Result struct {
	Stdout    string
	Stderr    string
	Retryable bool
}

func RunShell(ctx context.Context, a ShellArgs) (Result, error) {
	if a.Command == "" {
		return Result{}, fmt.Errorf("shell: command required")
	}
	to := time.Duration(a.TimeoutSec) * time.Second
	if to <= 0 {
		to = 30 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()

	cmd := exec.CommandContext(cctx, "/bin/sh", "-c", a.Command)
	out, err := cmd.CombinedOutput()
	res := Result{Stdout: string(out), Stderr: "", Retryable: false}

	if cctx.Err() == context.DeadlineExceeded {
		return res, fmt.Errorf("shell: timeout after %v", to)
	}
	if err != nil {
		// Consider non-zero exit codes as non-retryable by default.
		return res, fmt.Errorf("shell: %v; output=%q", err, string(out))
	}
	return res, nil
}
