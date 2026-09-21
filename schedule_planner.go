package cron

import (
	"sort"
	"time"
)

// schedulePlanner is the single owner of schedule arithmetic.
//
// It holds the set of entries and is the only component that is allowed to
// inspect a Schedule, compute an entry's next activation time, order entries
// by their next activation, or decide which entries are due at a given time.
// Keeping all of that here means the triggering layer never re-derives a
// schedule on its own; it asks the planner.
//
// A schedulePlanner is not safe for concurrent use. The Cron drive loop is
// its sole driver, and the pre-running API calls are serialized by Cron's
// runningMu.
type schedulePlanner struct {
	entries  []*Entry
	location *time.Location
}

func newSchedulePlanner(location *time.Location) *schedulePlanner {
	return &schedulePlanner{location: location}
}

// prime computes the first activation time of every known entry. It is the
// only activation computation performed outside of the drive loop (once, when
// the loop starts), and it reports each result so the driver can emit the
// original "schedule" log line.
func (p *schedulePlanner) prime(now time.Time, observe func(*Entry)) {
	for _, entry := range p.entries {
		entry.Next = entry.Schedule.Next(now)
		if observe != nil {
			observe(entry)
		}
	}
}

// add registers an entry and records its next activation.
func (p *schedulePlanner) add(entry *Entry, now time.Time) {
	entry.Next = entry.Schedule.Next(now)
	p.entries = append(p.entries, entry)
}

// append registers an entry without computing an activation time. It is used
// before the scheduler is running; activation times are computed by prime.
func (p *schedulePlanner) append(entry *Entry) {
	p.entries = append(p.entries, entry)
}

// remove deletes the entry with the given ID. Removing a missing ID is a
// no-op, matching the previous behavior.
func (p *schedulePlanner) remove(id EntryID) {
	var kept []*Entry
	for _, entry := range p.entries {
		if entry.ID != id {
			kept = append(kept, entry)
		}
	}
	p.entries = kept
}

// sort orders entries by their next activation time, with unsatisfiable
// entries (a zero Next time) at the end.
func (p *schedulePlanner) sort() {
	sort.Sort(byTime(p.entries))
}

// len returns the number of tracked entries.
func (p *schedulePlanner) len() int {
	return len(p.entries)
}

// earliest returns the next activation time of the soonest entry, or the zero
// time when there are no satisfiable entries. The entries must be sorted.
func (p *schedulePlanner) earliest() time.Time {
	if len(p.entries) == 0 {
		return time.Time{}
	}
	return p.entries[0].Next
}

// due returns the leading run of entries whose next activation is at or before
// now, in sorted order. The entries must be sorted.
func (p *schedulePlanner) due(now time.Time) []*Entry {
	var ready []*Entry
	for _, entry := range p.entries {
		if entry.Next.After(now) || entry.Next.IsZero() {
			break
		}
		ready = append(ready, entry)
	}
	return ready
}

// advance records that an entry has just fired at now and asks the schedule
// for its following activation. It is the one place Prev is shifted onto the
// previous Next.
func (p *schedulePlanner) advance(entry *Entry, now time.Time) {
	entry.Prev = entry.Next
	entry.Next = entry.Schedule.Next(now)
}

// each visits every entry in storage order.
func (p *schedulePlanner) each(visit func(*Entry)) {
	for _, entry := range p.entries {
		visit(entry)
	}
}

// snapshot returns a value copy of every entry, safe to hand to callers.
func (p *schedulePlanner) snapshot() []Entry {
	entries := make([]Entry, len(p.entries))
	for i, entry := range p.entries {
		entries[i] = *entry
	}
	return entries
}

// byTime sorts entries by their next activation time (with zero time last).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}
