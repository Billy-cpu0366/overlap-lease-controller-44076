package cron

import (
	"sort"
	"time"
)

// EntryID identifies an entry within a Cron instance
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
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

// entryStore owns the set of entries and every decision about their
// activation times: planning initial activations, ordering, determining
// which entries are due, and advancing entries after they fire. It is the
// single place where schedules are turned into concrete activation times,
// so the run loop never recomputes them on its own.
//
// An entryStore is not safe for concurrent use; the Cron run loop and the
// runningMu-guarded paths in cron.go provide the synchronization.
type entryStore struct {
	entries []*Entry
	logger  Logger
}

// newEntryStore returns an entry store that logs planning decisions to the
// given logger.
func newEntryStore(logger Logger) *entryStore {
	return &entryStore{logger: logger}
}

// plan computes the initial next activation time for every entry.
func (s *entryStore) plan(now time.Time) {
	for _, entry := range s.entries {
		entry.Next = entry.Schedule.Next(now)
		s.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}
}

// insert adds a new entry, computing its next activation time against now.
func (s *entryStore) insert(entry *Entry, now time.Time) {
	entry.Next = entry.Schedule.Next(now)
	s.entries = append(s.entries, entry)
	s.logger.Info("added", "now", now, "entry", entry.ID, "next", entry.Next)
}

// appendRaw adds a new entry without computing its activation time. It is
// used when the scheduler is not running and no clock is available.
func (s *entryStore) appendRaw(entry *Entry) {
	s.entries = append(s.entries, entry)
}

// remove deletes the entry with the given ID, if present.
func (s *entryStore) remove(id EntryID) {
	var entries []*Entry
	for _, e := range s.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	s.entries = entries
}

// sortByTime orders the entries by their next activation time, with
// unsatisfiable (zero-time) entries at the end.
func (s *entryStore) sortByTime() {
	sort.Sort(byTime(s.entries))
}

// earliest returns the next activation time across all entries, or the zero
// time if there are no entries or none are satisfiable. It must be called
// after sortByTime.
func (s *entryStore) earliest() time.Time {
	if len(s.entries) == 0 {
		return time.Time{}
	}
	return s.entries[0].Next
}

// due returns the entries whose activation time has been reached as of now,
// in firing order. It must be called after sortByTime.
func (s *entryStore) due(now time.Time) []*Entry {
	var due []*Entry
	for _, e := range s.entries {
		if e.Next.After(now) || e.Next.IsZero() {
			break
		}
		due = append(due, e)
	}
	return due
}

// advance records that the entry fired at its scheduled time and computes
// its next activation.
func (s *entryStore) advance(e *Entry, now time.Time) {
	e.Prev = e.Next
	e.Next = e.Schedule.Next(now)
	s.logger.Info("run", "now", now, "entry", e.ID, "next", e.Next)
}

// snapshot returns a copy of the current entry list.
func (s *entryStore) snapshot() []Entry {
	var entries = make([]Entry, len(s.entries))
	for i, e := range s.entries {
		entries[i] = *e
	}
	return entries
}
