// +build !nomon

package mon

import (
	"strings"
	"sync/atomic"
	_ "unsafe"

	"github.com/zeebo/this"
)

//go:linkname nanotime runtime.nanotime
func nanotime() (mono int64)

// Times calls the callback with all of the histograms that have been captured.
func Times(cb func(string, *State) bool) {
	for iter := states.Iterator(); iter.Next(); {
		if !cb(iter.Key(), (*State)(iter.Value())) {
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
		t.val.Store(this.ThisN(2))
	}
	return StartNamed(t.val.Load().(string))
}

// Start returns a Timer using the calling function for the name.
func Start() Timer {
	return StartNamed(this.ThisN(2))
}

// StartNamed returns a Timer that records a duration when its Done method is called.
func StartNamed(name string) Timer {
	state := GetState(name)
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
func (r Timer) Stop(err *error) {
	type namer interface {
		Name() (string, bool)
	}

	kind := ""
	if err != nil {
		if n, ok := (*err).(namer); ok {
			if name, ok := n.Name(); ok {
				kind = name
			}
		}
		if kind == "" && *err != nil {
			s := (*err).Error()
			if i := strings.IndexByte(s, ':'); i > 0 {
				kind = s[:i]
			} else if strings.IndexByte(s, ' ') == -1 {
				kind = s
			} else {
				kind = "error"
			}
		}
	}

	r.state.done(nanotime()-r.now, kind)
}
