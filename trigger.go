package cron

import "time"

// timer is the narrow seam between the drive loop and the passage of time.
// The trigger owns nothing but waiting for a chosen instant and reporting it.
type timer interface {
	// waitFor (re)arms the timer to deliver after d.
	waitFor(d time.Duration)
	// wake returns the channel on which the activation time is delivered.
	wake() <-chan time.Time
	// stop releases the pending timer.
	stop()
}

// clockTimer adapts the standard library timer to the timer seam.
type clockTimer struct {
	timer *time.Timer
}

func (t *clockTimer) waitFor(d time.Duration) {
	t.timer = time.NewTimer(d)
}

func (t *clockTimer) wake() <-chan time.Time {
	return t.timer.C
}

func (t *clockTimer) stop() {
	t.timer.Stop()
}

// trigger turns a desired wait duration into a wake-up. It knows nothing
// about entries or schedules; the drive loop hands it a duration derived from
// the planner and the trigger only reports when that duration elapses.
type trigger struct {
	timer timer
}

func newTrigger() *trigger {
	return &trigger{timer: &clockTimer{}}
}

// arm starts waiting for d.
func (g *trigger) arm(d time.Duration) {
	g.timer.waitFor(d)
}

// wake returns the channel that receives the activation time.
func (g *trigger) wake() <-chan time.Time {
	return g.timer.wake()
}

// stop cancels the pending wait.
func (g *trigger) stop() {
	g.timer.stop()
}

// idleWait is how long to sleep when there is nothing ready to schedule. It
// still reacts promptly to additions, removals and stops because the drive
// loop selects on those channels alongside the timer.
const idleWait = 100000 * time.Hour
