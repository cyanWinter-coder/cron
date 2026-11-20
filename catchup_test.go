package cron

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitForCatchup polls cond until it holds or the deadline passes.
func waitForCatchup(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

// startCatchupCron builds a Cron with a minute schedule whose cursor is
// backdated by the given duration, and returns it along with the report
// channel. The cursor is truncated to the second so the missed activations
// are exactly cursor+1m, cursor+2m, ...
func startCatchupCron(store CursorStore, backdate time.Duration, runs *int64, opts ...Option) (*Cron, chan CatchupReport, time.Time) {
	reports := make(chan CatchupReport, 1)
	cursor := time.Now().Add(-backdate).Truncate(time.Second)
	if _, err := store.CommitCursor("job", cursor); err != nil {
		panic(err)
	}
	opts = append([]Option{
		WithCursorStore(store),
		WithCatchupReporter(func(r CatchupReport) { reports <- r }),
	}, opts...)
	c := New(opts...)
	c.ScheduleWithKey("job", Every(time.Minute), FuncJob(func() {
		atomic.AddInt64(runs, 1)
	}))
	c.Start()
	return c, reports, cursor
}

func awaitReport(t *testing.T, reports chan CatchupReport) CatchupReport {
	t.Helper()
	select {
	case r := <-reports:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for catchup report")
		return CatchupReport{}
	}
}

func TestCatchupAllClaimsEveryMissedActivation(t *testing.T) {
	store := NewMemoryCursorStore()
	var runs int64
	c, reports, cursor := startCatchupCron(store, 10*time.Minute, &runs,
		WithCatchupPolicy(CatchupAll))
	defer c.Stop()

	rep := awaitReport(t, reports)
	if len(rep.Claimed) != 10 {
		t.Fatalf("expected 10 claimed activations, got %d (%v)", len(rep.Claimed), rep.Claimed)
	}
	for i, claimed := range rep.Claimed {
		want := cursor.Add(time.Duration(i+1) * time.Minute)
		if !claimed.Equal(want) {
			t.Errorf("claimed[%d] = %v, want %v", i, claimed, want)
		}
		if i > 0 && !claimed.After(rep.Claimed[i-1]) {
			t.Errorf("claimed times not strictly increasing at %d", i)
		}
	}
	if len(rep.Dropped) != 0 {
		t.Errorf("expected no drops, got %v", rep.Dropped)
	}

	// Every claimed activation runs exactly once, through the job waiter.
	waitForCatchup(t, func() bool { return atomic.LoadInt64(&runs) == 10 }, "10 catchup runs")

	// The cursor advanced to the newest claimed activation.
	last := rep.Claimed[len(rep.Claimed)-1]
	if cur, _ := store.Cursor("job"); !cur.Equal(last) {
		t.Errorf("cursor = %v, want %v", cur, last)
	}

	// A new scheduler sharing the store must not re-claim those times.
	c.Stop()
	var runs2 int64
	c2, reports2, _ := startCatchupCron(store, 0, &runs2,
		WithCatchupPolicy(CatchupAll))
	defer c2.Stop()
	select {
	case r := <-reports2:
		t.Errorf("unexpected second catchup report: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	if n := atomic.LoadInt64(&runs2); n != 0 {
		t.Errorf("replayed %d already-claimed activations", n)
	}
}

func TestCatchupLastClaimsOnlyNewest(t *testing.T) {
	store := NewMemoryCursorStore()
	var runs int64
	c, reports, cursor := startCatchupCron(store, 10*time.Minute, &runs,
		WithCatchupPolicy(CatchupLast))
	defer c.Stop()

	rep := awaitReport(t, reports)
	if len(rep.Claimed) != 1 {
		t.Fatalf("expected 1 claimed activation, got %v", rep.Claimed)
	}
	if want := cursor.Add(10 * time.Minute); !rep.Claimed[0].Equal(want) {
		t.Errorf("claimed %v, want the newest missed activation %v", rep.Claimed[0], want)
	}
	if len(rep.Dropped) != 9 {
		t.Fatalf("expected 9 dropped activations, got %d", len(rep.Dropped))
	}
	for _, d := range rep.Dropped {
		if d.Reason != DropPolicy {
			t.Errorf("drop reason = %v, want %v", d.Reason, DropPolicy)
		}
	}
	waitForCatchup(t, func() bool { return atomic.LoadInt64(&runs) == 1 }, "1 catchup run")
}

func TestCatchupSkipByDefault(t *testing.T) {
	store := NewMemoryCursorStore()
	var runs int64
	// No WithCatchupPolicy: the zero value must preserve historical behavior.
	c, _, _ := startCatchupCron(store, 10*time.Minute, &runs)
	defer c.Stop()

	time.Sleep(300 * time.Millisecond)
	if n := atomic.LoadInt64(&runs); n != 0 {
		t.Errorf("ran %d missed activations with default policy", n)
	}
	if reps := c.CatchupReports(); len(reps) != 0 {
		t.Errorf("unexpected reports with default policy: %+v", reps)
	}
}

func TestCatchupMaxCatchupBoundsBacklog(t *testing.T) {
	store := NewMemoryCursorStore()
	var runs int64
	c, reports, cursor := startCatchupCron(store, 10*time.Minute, &runs,
		WithCatchupPolicy(CatchupAll), WithMaxCatchup(3))
	defer c.Stop()

	rep := awaitReport(t, reports)
	if len(rep.Claimed) != 3 {
		t.Fatalf("expected 3 claimed activations, got %v", rep.Claimed)
	}
	// The most recent activations are kept.
	for i, claimed := range rep.Claimed {
		want := cursor.Add(time.Duration(8+i) * time.Minute)
		if !claimed.Equal(want) {
			t.Errorf("claimed[%d] = %v, want %v", i, claimed, want)
		}
	}
	if len(rep.Dropped) != 7 {
		t.Fatalf("expected 7 dropped activations, got %d", len(rep.Dropped))
	}
	for _, d := range rep.Dropped {
		if d.Reason != DropOverflow {
			t.Errorf("drop reason = %v, want %v", d.Reason, DropOverflow)
		}
	}
	// The cursor advanced to the newest claimed activation only.
	if cur, _ := store.Cursor("job"); !cur.Equal(rep.Claimed[2]) {
		t.Errorf("cursor = %v, want %v", cur, rep.Claimed[2])
	}
}

func TestCatchupMaxLatenessDropsOldActivations(t *testing.T) {
	store := NewMemoryCursorStore()
	var runs int64
	c, reports, cursor := startCatchupCron(store, 10*time.Minute, &runs,
		WithCatchupPolicy(CatchupAll), WithMaxLateness(5*time.Minute))
	defer c.Stop()

	rep := awaitReport(t, reports)
	if len(rep.Claimed) != 5 {
		t.Fatalf("expected 5 claimed activations, got %v", rep.Claimed)
	}
	if len(rep.Dropped) != 5 {
		t.Fatalf("expected 5 dropped activations, got %d", len(rep.Dropped))
	}
	for _, d := range rep.Dropped {
		if d.Reason != DropLateness {
			t.Errorf("drop reason = %v, want %v", d.Reason, DropLateness)
		}
		if !d.Time.Before(rep.Now.Add(-5*time.Minute)) && d.Time != cursor.Add(time.Minute) {
			// sanity: dropped times are the older half
			t.Errorf("dropped time %v is not old", d.Time)
		}
	}
}

func TestCatchupZeroCursorStartsFresh(t *testing.T) {
	store := NewMemoryCursorStore()
	var runs int64
	reports := make(chan CatchupReport, 1)
	c := New(
		WithCatchupPolicy(CatchupAll),
		WithCursorStore(store),
		WithCatchupReporter(func(r CatchupReport) { reports <- r }),
	)
	// No cursor committed: there is no baseline to replay from.
	c.ScheduleWithKey("job", Every(time.Minute), FuncJob(func() {
		atomic.AddInt64(&runs, 1)
	}))
	c.Start()
	defer c.Stop()

	select {
	case r := <-reports:
		t.Errorf("unexpected catchup report without a cursor: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	if n := atomic.LoadInt64(&runs); n != 0 {
		t.Errorf("ran %d activations without a cursor baseline", n)
	}
}

func TestCatchupClockBackwardProducesNoMissed(t *testing.T) {
	store := NewMemoryCursorStore()
	var runs int64
	reports := make(chan CatchupReport, 1)
	// Cursor in the future, as if the clock was dialed back after the last
	// commit: nothing may be claimed.
	future := time.Now().Add(time.Hour)
	if _, err := store.CommitCursor("job", future); err != nil {
		t.Fatal(err)
	}
	c := New(
		WithCatchupPolicy(CatchupAll),
		WithCursorStore(store),
		WithCatchupReporter(func(r CatchupReport) { reports <- r }),
	)
	c.ScheduleWithKey("job", Every(time.Minute), FuncJob(func() {
		atomic.AddInt64(&runs, 1)
	}))
	c.Start()
	defer c.Stop()

	select {
	case r := <-reports:
		t.Errorf("unexpected catchup report with future cursor: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	if n := atomic.LoadInt64(&runs); n != 0 {
		t.Errorf("ran %d activations with a future cursor", n)
	}
	if cur, _ := store.Cursor("job"); !cur.Equal(future) {
		t.Errorf("cursor moved backwards to %v", cur)
	}
}

func TestCatchupRunsThroughChainAndJobWaiter(t *testing.T) {
	store := NewMemoryCursorStore()
	var wrapped, started, finished int64
	release := make(chan struct{})
	reports := make(chan CatchupReport, 1)
	cursor := time.Now().Add(-3 * time.Minute).Truncate(time.Second)
	if _, err := store.CommitCursor("job", cursor); err != nil {
		t.Fatal(err)
	}
	c := New(
		WithCatchupPolicy(CatchupAll),
		WithCursorStore(store),
		WithCatchupReporter(func(r CatchupReport) { reports <- r }),
		WithChain(func(j Job) Job {
			return FuncJob(func() {
				atomic.AddInt64(&wrapped, 1)
				j.Run()
			})
		}),
	)
	c.ScheduleWithKey("job", Every(time.Minute), FuncJob(func() {
		atomic.AddInt64(&started, 1)
		<-release
		atomic.AddInt64(&finished, 1)
	}))
	c.Start()

	rep := awaitReport(t, reports)
	if len(rep.Claimed) != 3 {
		t.Fatalf("expected 3 claimed activations, got %v", rep.Claimed)
	}
	waitForCatchup(t, func() bool { return atomic.LoadInt64(&started) == 3 }, "3 started jobs")

	// Stop while the catch-up jobs are blocked: the returned context must
	// not complete until they finish (Join semantics via the job waiter).
	ctx := c.Stop()
	select {
	case <-ctx.Done():
		t.Fatal("stop context completed while catchup jobs were still running")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-ctx.Done()

	if n := atomic.LoadInt64(&finished); n != 3 {
		t.Errorf("finished = %d, want 3", n)
	}
	if n := atomic.LoadInt64(&wrapped); n != 3 {
		t.Errorf("chain wrapper invocations = %d, want 3", n)
	}
}

// blockingStore blocks the second commit until released, so a test can stop
// the scheduler in the middle of catch-up dispatch.
type blockingStore struct {
	*MemoryCursorStore
	commits int64
	signal  chan struct{}
	release chan struct{}
}

func (s *blockingStore) CommitCursor(key string, t time.Time) (bool, error) {
	if atomic.AddInt64(&s.commits, 1) == 2 {
		close(s.signal)
		<-s.release
	}
	return s.MemoryCursorStore.CommitCursor(key, t)
}

func TestCatchupStopDuringDispatch(t *testing.T) {
	store := &blockingStore{
		MemoryCursorStore: NewMemoryCursorStore(),
		signal:            make(chan struct{}),
		release:           make(chan struct{}),
	}
	var runs int64
	reports := make(chan CatchupReport, 1)
	cursor := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	if _, err := store.CommitCursor("job", cursor); err != nil {
		t.Fatal(err)
	}
	// The seed commit above must not count towards the blocking commit.
	atomic.StoreInt64(&store.commits, 0)
	c := New(
		WithCatchupPolicy(CatchupAll),
		WithCursorStore(store),
		WithCatchupReporter(func(r CatchupReport) { reports <- r }),
	)
	c.ScheduleWithKey("job", Every(time.Minute), FuncJob(func() {
		atomic.AddInt64(&runs, 1)
	}))
	c.Start()

	// Wait until the second claim is in flight, then stop the scheduler.
	<-store.signal
	stopped := make(chan context.Context, 1)
	go func() { stopped <- c.Stop() }()
	// Give Stop a moment to park on the stop channel, then let dispatch
	// continue: it must observe the stop instead of claiming more.
	time.Sleep(100 * time.Millisecond)
	close(store.release)
	ctx := <-stopped
	<-ctx.Done()

	rep := awaitReport(t, reports)
	if len(rep.Claimed) != 2 {
		t.Fatalf("expected 2 claimed activations before shutdown, got %v", rep.Claimed)
	}
	if len(rep.Dropped) != 8 {
		t.Fatalf("expected 8 dropped activations after shutdown, got %d", len(rep.Dropped))
	}
	for _, d := range rep.Dropped {
		if d.Reason != DropShutdown {
			t.Errorf("drop reason = %v, want %v", d.Reason, DropShutdown)
		}
	}
	// The cursor advanced only past activations that were actually claimed.
	if cur, _ := store.Cursor("job"); !cur.Equal(rep.Claimed[1]) {
		t.Errorf("cursor = %v, want last claimed %v", cur, rep.Claimed[1])
	}
}

func TestNormalRunsCommitCursor(t *testing.T) {
	store := NewMemoryCursorStore()
	c := New(
		WithCatchupPolicy(CatchupAll),
		WithCursorStore(store),
	)
	c.ScheduleWithKey("job", Every(time.Second), FuncJob(func() {}))
	c.Start()
	defer c.Stop()

	waitForCatchup(t, func() bool {
		cur, _ := store.Cursor("job")
		return !cur.IsZero()
	}, "cursor commit from a regular run")

	cur, _ := store.Cursor("job")
	if cur.After(time.Now()) {
		t.Errorf("cursor %v is in the future", cur)
	}
}

func TestMemoryCursorStoreIsMonotonic(t *testing.T) {
	store := NewMemoryCursorStore()
	now := time.Now()

	if ok, _ := store.CommitCursor("k", now); !ok {
		t.Error("first commit rejected")
	}
	if ok, _ := store.CommitCursor("k", now.Add(-time.Minute)); ok {
		t.Error("stale commit accepted")
	}
	if ok, _ := store.CommitCursor("k", now); ok {
		t.Error("equal commit accepted")
	}
	if ok, _ := store.CommitCursor("k", now.Add(time.Minute)); !ok {
		t.Error("newer commit rejected")
	}
	if cur, _ := store.Cursor("k"); !cur.Equal(now.Add(time.Minute)) {
		t.Errorf("cursor = %v, want %v", cur, now.Add(time.Minute))
	}
	if cur, _ := store.Cursor("missing"); !cur.IsZero() {
		t.Errorf("cursor for unknown key = %v, want zero", cur)
	}
}

func TestFileCursorStorePersistsAndRejectsStaleCommits(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/cursors.json"
	now := time.Now().Truncate(time.Millisecond)

	stale, err := NewFileCursorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := NewFileCursorStore(path)
	if err != nil {
		t.Fatal(err)
	}

	// The "fresh" process commits first.
	if ok, err := fresh.CommitCursor("job", now); !ok || err != nil {
		t.Fatalf("fresh commit: ok=%v err=%v", ok, err)
	}
	// The "stale" process, started before that commit, must not overwrite it
	// with an older cursor.
	if ok, err := stale.CommitCursor("job", now.Add(-time.Minute)); ok || err != nil {
		t.Fatalf("stale commit: ok=%v err=%v, want ok=false", ok, err)
	}

	// The committed value survived on disk and is observed by a new store.
	reloaded, err := NewFileCursorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if cur, _ := reloaded.Cursor("job"); !cur.Equal(now) {
		t.Errorf("reloaded cursor = %v, want %v", cur, now)
	}
	// A newer commit from the stale process is still accepted.
	if ok, err := stale.CommitCursor("job", now.Add(time.Minute)); !ok || err != nil {
		t.Fatalf("newer commit from stale process: ok=%v err=%v", ok, err)
	}
}

func TestCatchupStoreErrorAbortsClaims(t *testing.T) {
	store := &failingStore{CursorStore: NewMemoryCursorStore()}
	var runs int64
	reports := make(chan CatchupReport, 1)
	cursor := time.Now().Add(-3 * time.Minute).Truncate(time.Second)
	if _, err := NewMemoryCursorStore().Cursor("job"); err != nil {
		t.Fatal(err)
	}
	store.seed("job", cursor)
	c := New(
		WithCatchupPolicy(CatchupAll),
		WithCursorStore(store),
		WithCatchupReporter(func(r CatchupReport) { reports <- r }),
	)
	c.ScheduleWithKey("job", Every(time.Minute), FuncJob(func() {
		atomic.AddInt64(&runs, 1)
	}))
	c.Start()
	defer c.Stop()

	rep := awaitReport(t, reports)
	if len(rep.Claimed) != 0 {
		t.Errorf("claimed %v despite store errors", rep.Claimed)
	}
	if len(rep.Dropped) != 3 {
		t.Fatalf("expected 3 dropped activations, got %d", len(rep.Dropped))
	}
	for _, d := range rep.Dropped {
		if d.Reason != DropError {
			t.Errorf("drop reason = %v, want %v", d.Reason, DropError)
		}
	}
	if n := atomic.LoadInt64(&runs); n != 0 {
		t.Errorf("ran %d jobs despite store errors", n)
	}
}

type failingStore struct {
	CursorStore
	mu sync.Mutex
}

func (s *failingStore) seed(key string, t time.Time) {
	s.CursorStore.(*MemoryCursorStore).cursors[key] = t
}

func (s *failingStore) CommitCursor(key string, t time.Time) (bool, error) {
	return false, errStore
}

type storeError string

func (e storeError) Error() string { return string(e) }

const errStore = storeError("store unavailable")
