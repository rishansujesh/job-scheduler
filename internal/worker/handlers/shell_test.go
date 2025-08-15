package handlers

import (
	"context"
	"testing"
)

func TestRunShell_MissingCommand(t *testing.T) {
	_, err := RunShell(context.Background(), ShellArgs{})
	if err == nil {
		t.Fatalf("expected error for missing command")
	}
}
