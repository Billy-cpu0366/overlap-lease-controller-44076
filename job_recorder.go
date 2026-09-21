package cron

import "sync"

// jobRecorder is the single place that decides where a due job goes: it
// launches the already-wrapped job in its own goroutine and tracks the set of
// in-flight jobs so a shutdown can wait for them. It contains no scheduling
// logic and never rewraps a job; wrapping happens once, in the same order as
// before, when the entry is submitted.
type jobRecorder struct {
	waiting sync.WaitGroup
}

func newJobRecorder() *jobRecorder {
	return &jobRecorder{}
}

// launch runs the job in a new goroutine, recording it as in-flight until it
// returns.
func (r *jobRecorder) launch(job Job) {
	r.waiting.Add(1)
	go func() {
		defer r.waiting.Done()
		job.Run()
	}()
}

// wait blocks until every launched job has finished.
func (r *jobRecorder) wait() {
	r.waiting.Wait()
}
