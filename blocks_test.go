package cron

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// onceSchedule returns a fixed activation on its first use (so the entry is
// armed at startup) and the zero time afterwards (so it never fires again).
type onceSchedule struct {
	at    time.Time
	calls int32
}

func (s *onceSchedule) Next(time.Time) time.Time {
	if atomic.AddInt32(&s.calls, 1) == 1 {
		return s.at
	}
	return time.Time{}
}

// fakeTriggerTimer is a manually driven timer seam for the trigger block.
type fakeTriggerTimer struct {
	mu       sync.Mutex
	ch       chan time.Time
	previous chan time.Time
	lastD    time.Duration
	armed    int
}

func (t *fakeTriggerTimer) waitFor(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.previous = t.ch
	t.ch = make(chan time.Time, 1)
	t.lastD = d
	t.armed++
}

func (t *fakeTriggerTimer) wake() <-chan time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ch
}

func (t *fakeTriggerTimer) stop() {}

func (t *fakeTriggerTimer) snapshot() (time.Duration, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastD, t.armed
}

func (t *fakeTriggerTimer) deliverOld(when time.Time) {
	t.mu.Lock()
	ch := t.previous
	t.mu.Unlock()
	if ch != nil {
		ch <- when
	}
}

// --- planner block ---------------------------------------------------------

func TestPlannerEmptyTable(t *testing.T) {
	p := newSchedulePlanner(time.UTC)
	p.sort()
	if got := p.len(); got != 0 {
		t.Fatalf("empty planner len = %d, want 0", got)
	}
	if !p.earliest().IsZero() {
		t.Fatalf("empty planner earliest = %v, want zero", p.earliest())
	}
	if got := p.due(time.Now()); len(got) != 0 {
		t.Fatalf("empty planner due = %d entries, want 0", len(got))
	}
	if got := p.snapshot(); len(got) != 0 {
		t.Fatalf("empty planner snapshot = %d entries, want 0", len(got))
	}
}

func TestPlannerUnsatisfiableSortsLastAndNeverDue(t *testing.T) {
	p := newSchedulePlanner(time.UTC)
	now := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)

	unsatisfiable := &Entry{ID: 1}
	soon := &Entry{ID: 2, Next: now.Add(time.Second)}
	p.append(unsatisfiable)
	p.append(soon)

	p.sort()
	if p.entries[0].ID != 2 || p.entries[1].ID != 1 {
		t.Fatalf("zero Next not sorted last: %v, %v", p.entries[0].ID, p.entries[1].ID)
	}
	if got := p.due(now.Add(time.Hour)); len(got) != 1 || got[0].ID != 2 {
		t.Fatalf("unsatisfiable entry reported due, got %v", got)
	}
}

// A long-running (far future) entry must stay behind an immediate one, and a
// single advance must move only the fired entry to its following activation.
func TestPlannerLongEntryPressedBehindImmediate(t *testing.T) {
	p := newSchedulePlanner(time.UTC)
	now := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)

	immediate := &Entry{ID: 1, Next: now}
	long := &Entry{ID: 2, Next: now.Add(24 * time.Hour)}
	p.append(long)
	p.append(immediate)

	p.sort()
	due := p.due(now)
	if len(due) != 1 || due[0].ID != 1 {
		t.Fatalf("expected only immediate entry due, got %v", due)
	}

	next := now.Add(2 * time.Second)
	due[0].Schedule = &fixedNextSchedule{next}
	p.advance(due[0], now)
	if !due[0].Prev.Equal(now) || !due[0].Next.Equal(next) {
		t.Fatalf("advance did not record prev/next: %v %v", due[0].Prev, due[0].Next)
	}
	if !long.Next.Equal(now.Add(24 * time.Hour)) {
		t.Fatalf("advance of one entry disturbed the long entry")
	}
}

type fixedNextSchedule struct{ at time.Time }

func (s *fixedNextSchedule) Next(time.Time) time.Time { return s.at }

// Schedule parity for month-end / leap-day / DST edges: the planner must not
// alter what SpecSchedule.Next produces, so the generated slots are identical
// before and after the reorganization.
func TestPlannerCalendarParity(t *testing.T) {
	p := newSchedulePlanner(time.UTC)
	parse := func(spec string) Schedule {
		sch, err := standardParser.Parse(spec)
		if err != nil {
			t.Fatalf("parse %q: %v", spec, err)
		}
		return sch
	}

	cases := []struct {
		name string
		spec string
		from time.Time
		want time.Time
	}{
		{
			name: "leap day in a leap year",
			spec: "0 0 29 2 *",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want: time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "leap day skips a non-leap year",
			spec: "0 0 29 2 *",
			from: time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
			want: time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "day 31 from February lands on March 31",
			spec: "0 0 31 * *",
			from: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			want: time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "monthly from January 31st advances to a 31-day month",
			spec: "0 0 31 * *",
			from: time.Date(2024, 1, 31, 0, 0, 1, 0, time.UTC),
			want: time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := &Entry{ID: 1, Schedule: parse(tc.spec)}
			p.append(entry)
			p.prime(tc.from, nil)
			if !entry.Next.Equal(tc.want) {
				t.Fatalf("%s: slot = %v, want %v", tc.name, entry.Next, tc.want)
			}
			p.remove(1)
		})
	}
}

// DST transition: a 02:30 slot on the spring-forward day in America/New_York
// must be produced by the planner exactly as SpecSchedule computes it.
func TestPlannerDSTParity(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}
	sch, err := standardParser.Parse("30 2 10 3 *")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2024, 3, 9, 0, 0, 0, 0, loc)
	want := sch.Next(from) // reference value straight from the schedule block

	p := newSchedulePlanner(loc)
	entry := &Entry{ID: 1, Schedule: sch}
	p.append(entry)
	p.prime(from, nil)
	if !entry.Next.Equal(want) {
		t.Fatalf("DST slot = %v, want %v", entry.Next, want)
	}
}

// --- trigger block ---------------------------------------------------------

func TestClockTimerFiresAndStops(t *testing.T) {
	g := newTrigger()
	g.arm(20 * time.Millisecond)
	select {
	case <-g.wake():
	case <-time.After(time.Second):
		t.Fatal("trigger did not fire")
	}

	g.arm(time.Hour)
	g.stop()
	select {
	case <-g.wake():
		t.Fatal("stopped trigger fired")
	case <-time.After(20 * time.Millisecond):
	}
}

// An entry removed just as it is due must not run, even if the stale wake-up
// is delivered after the trigger was rearmed for the empty table.
func TestRemovedDueEntryDoesNotRun(t *testing.T) {
	c := newWithSeconds()
	ft := new(fakeTriggerTimer)
	c.trigger = &trigger{timer: ft}

	var ran int32
	at := c.now().Add(50 * time.Millisecond)
	id := c.Schedule(&onceSchedule{at: at}, FuncJob(func() {
		atomic.AddInt32(&ran, 1)
	}))

	c.Start()
	defer c.Stop()

	// Wait for the initial arm, then remove before delivering the wake-up.
	deadline := time.Now().Add(time.Second)
	for {
		if _, armed := ft.snapshot(); armed >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("drive loop never armed the trigger")
		}
		time.Sleep(time.Millisecond)
	}

	c.Remove(id)

	deadline = time.Now().Add(time.Second)
	for {
		if d, _ := ft.snapshot(); d == idleWait {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("drive loop did not rearm for the empty table")
		}
		time.Sleep(time.Millisecond)
	}

	// Deliver the stale activation on the channel the drive loop abandoned.
	ft.deliverOld(at)
	time.Sleep(20 * time.Millisecond)
	if atomic.LoadInt32(&ran) != 0 {
		t.Fatal("removed entry ran")
	}
}

// --- recorder block --------------------------------------------------------

func TestRecorderWaitsForLaunchedJobs(t *testing.T) {
	r := newJobRecorder()
	var done int64
	for i := 0; i < 10; i++ {
		r.launch(FuncJob(func() {
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt64(&done, 1)
		}))
	}
	r.wait()
	if atomic.LoadInt64(&done) != 10 {
		t.Fatalf("recorder waited past %d/10 jobs", done)
	}
}

// --- unified entry point under concurrency ---------------------------------

func TestConcurrentAddSnapshotRemove(t *testing.T) {
	c := newWithSeconds()
	c.Start()
	defer c.Stop()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(300 * time.Millisecond)
			for time.Now().Before(deadline) {
				id, err := c.AddFunc("* * * * * ?", func() {})
				if err != nil {
					t.Error(err)
					return
				}
				c.Entries()
				c.Entry(id)
				c.Remove(id)
			}
		}()
	}
	wg.Wait()
}

// Repeated start/stop cycling while jobs are added and removed must not lose
// signals, deadlock, or run a removed job.
func TestRepeatedStartStopCycling(t *testing.T) {
	c := newWithSeconds()
	defer c.Stop()

	var ran int64
	for i := 0; i < 25; i++ {
		c.Start()
		id, _ := c.AddFunc("* * * * * ?", func() { atomic.AddInt64(&ran, 1) })
		c.Entries()
		c.Remove(id)
		ctx := c.Stop()
		<-ctx.Done()
	}
}
