package handlers

import (
	"context"
	"testing"
)

func TestRunHTTP_MissingURL(t *testing.T) {
	_, err := RunHTTP(context.Background(), HTTPArgs{Method: "GET"})
	if err == nil {
		t.Fatalf("expected error for missing URL")
	}
}
