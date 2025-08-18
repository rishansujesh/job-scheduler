package schedule

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// NextRun computes the next run time given either a cron expression OR fixed interval seconds.
// "from" is the reference time; timezone is applied if provided.
func NextRun(cronExpr *string, fixedIntervalSeconds *int, from time.Time, timezone string) (time.Time, error) {
	loc := time.UTC
	if timezone != "" {
		l, err := time.LoadLocation(timezone)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid timezone: %w", err)
		}
		loc = l
	}
	ref := from.In(loc)

	switch {
	case cronExpr != nil && *cronExpr != "":
		// Standard 5-field cron parser (minute hour dom month dow)
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		sched, err := parser.Parse(*cronExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid cron: %w", err)
		}
		return sched.Next(ref), nil

	case fixedIntervalSeconds != nil && *fixedIntervalSeconds > 0:
		return ref.Add(time.Duration(*fixedIntervalSeconds) * time.Second), nil

	default:
		return time.Time{}, fmt.Errorf("no schedule provided")
	}
}
