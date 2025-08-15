package jobs

import (
	"testing"
	"time"
)

func TestComputeIdempotencyKey_Deterministic(t *testing.T) {
	args := map[string]any{"k": "v"}
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	k1, err := ComputeIdempotencyKey("job-123", ts, args)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := ComputeIdempotencyKey("job-123", ts, args)
	if err != nil {
		t.Fatal(err)
	}
	if k1 != k2 {
		t.Fatalf("keys differ: %s vs %s", k1, k2)
	}
}

func TestComputeIdempotencyKey_ChangesWithInputs(t *testing.T) {
	args := map[string]any{"k": "v"}
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	k1, _ := ComputeIdempotencyKey("job-123", ts, args)
	ts2 := ts.Add(time.Second)
	k2, _ := ComputeIdempotencyKey("job-123", ts2, args)
	if k1 == k2 {
		t.Fatalf("expected different keys when time changes")
	}
}
