package schedule

import (
	"testing"
	"time"
)

func TestNextRun_FixedInterval(t *testing.T) {
	sec := 15
	from := time.Date(2025,1,1,0,0,0,0,time.UTC)
	nxt, err := NextRun(nil, &sec, from, "UTC")
	if err != nil { t.Fatalf("err: %v", err) }
	if want := from.Add(15*time.Second); !nxt.Equal(want) {
		t.Fatalf("want %v got %v", want, nxt)
	}
}

func TestNextRun_Cron(t *testing.T) {
	cron := "*/5 * * * *" // every 5 minutes
	from := time.Date(2025,1,1,0,2,0,0,time.UTC)
	nxt, err := NextRun(&cron, nil, from, "UTC")
	if err != nil { t.Fatalf("err: %v", err) }
	if want := time.Date(2025,1,1,0,5,0,0,time.UTC); !nxt.Equal(want) {
		t.Fatalf("want %v got %v", want, nxt)
	}
}
