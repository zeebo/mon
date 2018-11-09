// +build !nomon

package mon

import (
	"sync/atomic"
	_ "unsafe"
)

//go:linkname nanotime runtime.nanotime
func nanotime() (mono int64)

// Times calls the callback with all of the histograms that have been captured.
func Times(cb func(string, *State) bool) {
	for name, state := range loadState() {
		if !cb(name, state) {
			return
		}
	}
}

// Thunk is a type that allows one to get the benefits of Time without having to
// compute the caller every time it's called. Zero values are valid.
type Thunk struct {
	val atomic.Value
}

// Time returns a Timer where the name is chosen the first time by the caller. Don't
// use the same Thunk from different functions/methods.
func (t *Thunk) Start() Timer {
	if t.val.Load() == nil {
		t.val.Store(this2())
	}
	return StartNamed(t.val.Load().(string))
}

// Start returns a Timer using the calling function for the name.
func Start() Timer {
	return StartNamed(this2())
}

// StartNamed returns a Timer that records a duration when its Done method is called.
func StartNamed(name string) Timer {
	state, ok := loadState()[name]
	if !ok {
		state = newState(name)
	}

	state.start()
	return Timer{
		now:   nanotime(),
		state: state,
	}
}

// Timer keeps track of the state necessary to record timing info.
type Timer struct {
	now   int64
	state *State
}

// Stop records the timing info.
func (r Timer) Stop() { r.state.done(nanotime() - r.now) }
