package cron

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// CatchupPolicy decides how activations that were missed while the scheduler
// was down are recovered when it starts.
type CatchupPolicy int

const (
	// CatchupSkip ignores missed activations. It is the zero value and
	// preserves the historical behavior: only future activations run.
	CatchupSkip CatchupPolicy = iota

	// CatchupAll claims and runs every missed activation once, oldest first.
	CatchupAll

	// CatchupLast claims and runs only the most recent missed activation;
	// the remaining ones are reported as dropped.
	CatchupLast
)

func (p CatchupPolicy) String() string {
	switch p {
	case CatchupAll:
		return "all"
	case CatchupLast:
		return "last"
	default:
		return "skip"
	}
}

// DropReason explains why a missed activation was not claimed.
type DropReason string

const (
	// DropPolicy indicates the activation was excluded by the configured
	// CatchupPolicy (e.g. CatchupLast kept only the newest one).
	DropPolicy DropReason = "policy"

	// DropLateness indicates the activation was older than the maximum
	// lateness window (see WithMaxLateness).
	DropLateness DropReason = "lateness"

	// DropOverflow indicates the activation exceeded the maximum number of
	// catch-up runs (see WithMaxCatchup) or the enumeration safety cap.
	DropOverflow DropReason = "overflow"

	// DropShutdown indicates the scheduler was stopped before the activation
	// could be claimed.
	DropShutdown DropReason = "shutdown"

	// DropConflict indicates the activation was already claimed: the cursor
	// store reported a newer-or-equal committed cursor, e.g. written by
	// another process.
	DropConflict DropReason = "conflict"

	// DropError indicates the cursor store failed while claiming the
	// activation.
	DropError DropReason = "error"
)

// MissedDrop records a single missed activation that was not run, together
// with the reason why.
type MissedDrop struct {
	Time   time.Time
	Reason DropReason
}

// CatchupReport is the structured, auditable result of recovering missed
// activations for one entry at scheduler startup. A report is produced only
// when there is something to tell: claimed or dropped activations, a
// truncated enumeration, or a store error.
type CatchupReport struct {
	// EntryID and Key identify the entry the report belongs to.
	EntryID EntryID
	Key     string

	// Policy is the catch-up policy that was applied.
	Policy CatchupPolicy

	// Cursor is the committed cursor before recovery: activations at or
	// before it were already handled.
	Cursor time.Time

	// Now is the startup time the recovery window was computed against.
	Now time.Time

	// Claimed lists the missed activations that were claimed and dispatched
	// to the job, oldest first. Each of them runs exactly once.
	Claimed []time.Time

	// Dropped lists the missed activations that were not run, with reasons.
	Dropped []MissedDrop

	// Truncated indicates the backlog exceeded the enumeration safety cap;
	// no activations were claimed and activations beyond the cap were not
	// even inspected. Nothing is silently lost: the cursor is left untouched
	// so the backlog is re-examined on the next start.
	Truncated bool

	// Err is set when the cursor store failed before any claim was made.
	Err error
}

// CursorStore persists the catch-up cursor of each entry. The cursor marks
// the newest activation time that has been claimed; activations after it are
// candidates for recovery on the next start.
//
// Implementations must make CommitCursor atomic and monotonic: a commit only
// succeeds if it strictly advances the stored cursor. This is what prevents a
// stale record from a late, older process from overwriting a cursor that a
// newer process already committed, and what guarantees each activation time
// is claimed at most once.
type CursorStore interface {
	// Cursor returns the committed cursor for key, or the zero time if no
	// cursor was ever committed.
	Cursor(key string) (time.Time, error)

	// CommitCursor atomically advances the cursor for key to t. It reports
	// whether the commit succeeded; false means a newer-or-equal cursor was
	// already committed and the caller must treat the activation as claimed
	// by someone else.
	CommitCursor(key string, t time.Time) (bool, error)
}

// MemoryCursorStore is an in-process CursorStore. It is monotonic and safe
// for concurrent use, but does not survive restarts; use it for tests or when
// downtime recovery is not needed.
type MemoryCursorStore struct {
	mu      sync.Mutex
	cursors map[string]time.Time
}

// NewMemoryCursorStore returns an empty in-memory cursor store.
func NewMemoryCursorStore() *MemoryCursorStore {
	return &MemoryCursorStore{cursors: make(map[string]time.Time)}
}

// Cursor implements CursorStore.
func (s *MemoryCursorStore) Cursor(key string) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursors[key], nil
}

// CommitCursor implements CursorStore.
func (s *MemoryCursorStore) CommitCursor(key string, t time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.cursors[key]; ok && !t.After(cur) {
		return false, nil
	}
	s.cursors[key] = t
	return true, nil
}

// FileCursorStore is a CursorStore backed by a JSON file, so cursors survive
// process restarts. Commits re-read the file before writing, so a stale
// process cannot overwrite a cursor committed by a newer one; writes are
// atomic (write to a temporary file, then rename).
//
// The file offers best-effort coordination between processes on the same
// host. For coordination across hosts, implement CursorStore on top of a
// compare-and-swap capable backend (e.g. a database or etcd).
type FileCursorStore struct {
	mu      sync.Mutex
	path    string
	cursors map[string]time.Time
}

// NewFileCursorStore opens (or creates on first commit) a cursor store file.
func NewFileCursorStore(path string) (*FileCursorStore, error) {
	s := &FileCursorStore{path: path, cursors: make(map[string]time.Time)}
	onDisk, err := readCursorFile(path)
	if err != nil {
		return nil, err
	}
	for k, v := range onDisk {
		s.cursors[k] = v
	}
	return s, nil
}

// Cursor implements CursorStore.
func (s *FileCursorStore) Cursor(key string) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursors[key], nil
}

// CommitCursor implements CursorStore. The file is re-read before the
// compare-and-swap so that commits from other processes are observed.
func (s *FileCursorStore) CommitCursor(key string, t time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	onDisk, err := readCursorFile(s.path)
	if err != nil {
		return false, err
	}
	// Merge: for every key, the committed value is the newest known cursor.
	for k, v := range onDisk {
		if cur, ok := s.cursors[k]; !ok || v.After(cur) {
			s.cursors[k] = v
		}
	}
	if cur, ok := s.cursors[key]; ok && !t.After(cur) {
		return false, nil
	}
	s.cursors[key] = t
	if err := writeCursorFile(s.path, s.cursors); err != nil {
		return false, err
	}
	return true, nil
}

func readCursorFile(path string) (map[string]time.Time, error) {
	data, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cursors map[string]time.Time
	if err := json.Unmarshal(data, &cursors); err != nil {
		return nil, err
	}
	return cursors, nil
}

func writeCursorFile(path string, cursors map[string]time.Time) error {
	data, err := json.Marshal(cursors)
	if err != nil {
		return err
	}
	tmp, err := ioutil.TempFile(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

// maxMissedEnumeration caps how many missed activations are enumerated for a
// single entry at startup. It guards against pathological backlogs (e.g. a
// per-second schedule after weeks of downtime). When the cap is hit, nothing
// is claimed and the report is marked truncated.
const maxMissedEnumeration = 10000

// entryKey returns the cursor-store key for an entry. Entries scheduled with
// an explicit key (ScheduleWithKey and friends) use it; others fall back to
// a per-process key derived from the entry ID, which is sufficient for the
// in-memory store but not across restarts.
func (c *Cron) entryKey(e *Entry) string {
	if e.Key != "" {
		return e.Key
	}
	return "cron-entry-" + strconv.Itoa(int(e.ID))
}

// runCatchup recovers missed activations for all entries against the startup
// time now. It returns true if the scheduler was asked to stop while
// recovering, in which case run must return immediately.
func (c *Cron) runCatchup(now time.Time) (stopRequested bool) {
	for _, e := range c.entries {
		if c.catchupEntry(e, now) {
			return true
		}
	}
	return false
}

// catchupEntry computes, claims and dispatches the missed activations of a
// single entry, and records the resulting report. It returns true if a stop
// was requested mid-recovery.
func (c *Cron) catchupEntry(e *Entry, now time.Time) (stopRequested bool) {
	report := CatchupReport{
		EntryID: e.ID,
		Key:     c.entryKey(e),
		Policy:  c.catchupPolicy,
		Now:     now,
	}

	cursor, err := c.cursorStore.Cursor(report.Key)
	if err != nil {
		report.Err = err
		c.recordCatchup(report)
		c.logger.Error(err, "catchup: cursor load failed", "entry", e.ID, "key", report.Key)
		return false
	}
	report.Cursor = cursor

	// Without a committed cursor there is no known baseline: replaying from
	// the zero time would enumerate an unbounded backlog. Use the lateness
	// window as the baseline if one is configured, otherwise start fresh.
	baseline := cursor
	if baseline.IsZero() {
		if c.maxLateness > 0 {
			baseline = now.Add(-c.maxLateness)
		} else {
			baseline = now
		}
	}

	// Enumerate the missed activations in (baseline, now]. The schedule
	// contract guarantees strictly increasing times; the guard protects
	// against schedules that violate it, so a clock skew or a broken
	// schedule can never produce duplicate activations or spin forever.
	var missed []time.Time
	truncated := false
	for t := baseline; ; {
		t = e.Schedule.Next(t)
		if t.IsZero() || t.After(now) {
			break
		}
		if len(missed) > 0 && !t.After(missed[len(missed)-1]) {
			break
		}
		missed = append(missed, t)
		if len(missed) >= maxMissedEnumeration {
			if nt := e.Schedule.Next(t); !nt.IsZero() && !nt.After(now) {
				truncated = true
			}
			break
		}
	}

	if len(missed) == 0 && !truncated {
		return false
	}
	report.Truncated = truncated

	keep := missed
	if truncated {
		// The backlog is too large to reason about safely. Claim nothing;
		// leave the cursor where it is so the next start re-examines it.
		for _, t := range keep {
			report.Dropped = append(report.Dropped, MissedDrop{Time: t, Reason: DropOverflow})
		}
		c.recordCatchup(report)
		c.logger.Info("catchup: backlog truncated", "entry", e.ID, "key", report.Key, "enumerated", len(missed))
		return false
	}

	// Bound the backlog by lateness: activations older than the window are
	// dropped, not claimed.
	if c.maxLateness > 0 {
		cutoff := now.Add(-c.maxLateness)
		var recent []time.Time
		for _, t := range keep {
			if t.Before(cutoff) {
				report.Dropped = append(report.Dropped, MissedDrop{Time: t, Reason: DropLateness})
			} else {
				recent = append(recent, t)
			}
		}
		keep = recent
	}

	// Apply the policy.
	if c.catchupPolicy == CatchupLast && len(keep) > 1 {
		for _, t := range keep[:len(keep)-1] {
			report.Dropped = append(report.Dropped, MissedDrop{Time: t, Reason: DropPolicy})
		}
		keep = keep[len(keep)-1:]
	}

	// Bound the backlog by count: keep the most recent activations.
	if c.maxCatchup > 0 && len(keep) > c.maxCatchup {
		for _, t := range keep[:len(keep)-c.maxCatchup] {
			report.Dropped = append(report.Dropped, MissedDrop{Time: t, Reason: DropOverflow})
		}
		keep = keep[len(keep)-c.maxCatchup:]
	}

	// Claim and dispatch, oldest first. The cursor only advances when an
	// activation is actually claimed; dropped activations leave it behind so
	// they remain visible to the next start.
	for i, t := range keep {
		select {
		case <-c.stop:
			// Shutdown in progress: claim nothing further.
			for _, remaining := range keep[i:] {
				report.Dropped = append(report.Dropped, MissedDrop{Time: remaining, Reason: DropShutdown})
			}
			c.recordCatchup(report)
			return true
		default:
		}

		ok, err := c.cursorStore.CommitCursor(report.Key, t)
		if err != nil {
			for _, failed := range keep[i:] {
				report.Dropped = append(report.Dropped, MissedDrop{Time: failed, Reason: DropError})
			}
			c.recordCatchup(report)
			c.logger.Error(err, "catchup: cursor commit failed", "entry", e.ID, "key", report.Key, "time", t)
			return false
		}
		if !ok {
			// A newer cursor was already committed (e.g. by another
			// process): this activation is claimed, just not by us.
			report.Dropped = append(report.Dropped, MissedDrop{Time: t, Reason: DropConflict})
			continue
		}
		// Claimed: dispatch through the same chain and job waiter as
		// regularly scheduled runs.
		c.startJob(e.WrappedJob)
		report.Claimed = append(report.Claimed, t)
		c.logger.Info("catchup: claimed missed activation", "entry", e.ID, "key", report.Key, "time", t)
	}

	c.recordCatchup(report)
	return false
}

// commitCursor advances the catch-up cursor of an entry after one of its
// regular activations has been handed off to a job goroutine. Failures are
// logged but never affect scheduling.
func (c *Cron) commitCursor(e *Entry, t time.Time) {
	if c.cursorStore == nil || c.catchupPolicy == CatchupSkip {
		return
	}
	ok, err := c.cursorStore.CommitCursor(c.entryKey(e), t)
	if err != nil {
		c.logger.Error(err, "catchup: cursor commit failed", "entry", e.ID, "time", t)
	} else if !ok {
		c.logger.Info("catchup: cursor commit superseded", "entry", e.ID, "time", t)
	}
}

// recordCatchup stores a report and notifies the reporter, if any. Empty
// reports are discarded.
func (c *Cron) recordCatchup(report CatchupReport) {
	if report.Err == nil && len(report.Claimed) == 0 && len(report.Dropped) == 0 && !report.Truncated {
		return
	}
	c.catchupMu.Lock()
	c.catchupReports = append(c.catchupReports, report)
	c.catchupMu.Unlock()
	if c.catchupReporter != nil {
		c.catchupReporter(report)
	}
}

// CatchupReports returns the catch-up reports recorded since this Cron was
// created, in the order they were produced.
func (c *Cron) CatchupReports() []CatchupReport {
	c.catchupMu.Lock()
	defer c.catchupMu.Unlock()
	reports := make([]CatchupReport, len(c.catchupReports))
	copy(reports, c.catchupReports)
	return reports
}
