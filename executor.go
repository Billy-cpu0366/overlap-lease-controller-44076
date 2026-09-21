package cron

import "sync"

// jobExecutor runs activated jobs in their own goroutines and tracks them so
// that a stopping scheduler can wait for in-flight jobs to finish. It is the
// only component that starts job goroutines.
type jobExecutor struct {
	wg sync.WaitGroup
}

// start runs the given job in a new goroutine.
func (e *jobExecutor) start(j Job) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		j.Run()
	}()
}

// wait blocks until all started jobs have completed.
func (e *jobExecutor) wait() {
	e.wg.Wait()
}
