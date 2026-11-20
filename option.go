package cron

import (
	"time"
)

// Option represents a modification to the default behavior of a Cron.
type Option func(*Cron)

// WithLocation overrides the timezone of the cron instance.
func WithLocation(loc *time.Location) Option {
	return func(c *Cron) {
		c.location = loc
	}
}

// WithSeconds overrides the parser used for interpreting job schedules to
// include a seconds field as the first one.
func WithSeconds() Option {
	return WithParser(NewParser(
		Second | Minute | Hour | Dom | Month | Dow | Descriptor,
	))
}

// WithParser overrides the parser used for interpreting job schedules.
func WithParser(p ScheduleParser) Option {
	return func(c *Cron) {
		c.parser = p
	}
}

// WithChain specifies Job wrappers to apply to all jobs added to this cron.
// Refer to the Chain* functions in this package for provided wrappers.
func WithChain(wrappers ...JobWrapper) Option {
	return func(c *Cron) {
		c.chain = NewChain(wrappers...)
	}
}

// WithLogger uses the provided logger.
func WithLogger(logger Logger) Option {
	return func(c *Cron) {
		c.logger = logger
	}
}

// WithCatchupPolicy sets how activations missed while the scheduler was down
// are recovered at startup. The default, CatchupSkip, preserves the
// historical behavior of running only future activations.
//
// A cursor store is required for recovery to take place; see WithCursorStore.
func WithCatchupPolicy(policy CatchupPolicy) Option {
	return func(c *Cron) {
		c.catchupPolicy = policy
	}
}

// WithCursorStore sets the store used to persist per-entry catch-up cursors.
// It is required for catch-up recovery (see WithCatchupPolicy) to activate.
// Use FileCursorStore to recover activations missed during downtime, or
// MemoryCursorStore when only in-process deduplication is needed.
func WithCursorStore(store CursorStore) Option {
	return func(c *Cron) {
		c.cursorStore = store
	}
}

// WithMaxCatchup bounds how many missed activations a single entry claims at
// startup. When the backlog is larger, the most recent activations are kept
// and the rest are reported as dropped with reason DropOverflow. Zero (the
// default) means no limit.
func WithMaxCatchup(n int) Option {
	return func(c *Cron) {
		c.maxCatchup = n
	}
}

// WithMaxLateness drops missed activations older than the given window before
// the catch-up policy is applied; they are reported with reason DropLateness.
// Zero (the default) means no lateness limit.
func WithMaxLateness(d time.Duration) Option {
	return func(c *Cron) {
		c.maxLateness = d
	}
}

// WithCatchupReporter registers a callback invoked with each CatchupReport
// produced at startup, in addition to the reports being available via
// CatchupReports. The callback runs on the scheduler goroutine and must not
// block.
func WithCatchupReporter(reporter func(CatchupReport)) Option {
	return func(c *Cron) {
		c.catchupReporter = reporter
	}
}
