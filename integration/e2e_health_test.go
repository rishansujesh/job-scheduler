package integration

import (
	"net/http"
	"os"
	"testing"
	"time"
)

func TestAPIHealthz(t *testing.T) {
	if os.Getenv("E2E") == "" {
		t.Skip("set E2E=1 to run end-to-end tests")
	}
	client := &http.Client{ Timeout: 3 * time.Second }
	resp, err := client.Get("http://localhost:8080/healthz")
	if err != nil { t.Fatalf("healthz request failed: %v", err) }
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
