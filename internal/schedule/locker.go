package schedule

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/binary"
)

// WithScheduleTxLock opens a transaction and attempts a pg_try_advisory_xact_lock on a hash of scheduleID.
// If the lock is obtained, it calls fn(tx) and commits; otherwise returns (false, nil).
func WithScheduleTxLock(ctx context.Context, db *sql.DB, scheduleID string, fn func(*sql.Tx) error) (bool, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	key := hashToBigInt(scheduleID)
	var locked bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock($1)`, key).Scan(&locked); err != nil {
		return false, err
	}
	if !locked {
		return false, nil
	}

	if err := fn(tx); err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// hashToBigInt produces a signed 64-bit integer from the schedule id string.
func hashToBigInt(s string) int64 {
	h := sha1.Sum([]byte(s))
	return int64(binary.BigEndian.Uint64(h[0:8]))
}
