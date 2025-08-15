package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// ComputeIdempotencyKey = sha256(job_id + scheduled_time_iso + args_json_sorted)
func ComputeIdempotencyKey(jobID string, scheduled time.Time, args map[string]any) (string, error) {
	// marshal args deterministically
	b, err := json.Marshal(args)
	if err != nil {
		return "", err
	}
	payload := jobID + scheduled.UTC().Format(time.RFC3339Nano) + string(b)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:]), nil
}

// Small helper to put a timeout around DB operations consistently.
func WithTimeout(ctx context.Context, d time.Duration) (context.Context, func()) {
	return context.WithTimeout(ctx, d)
}
