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

// Thunk is a type that allows one to get the benefits of Time without having to
// compute the caller every time it's called. Zero values are valid.
type Thunk struct {
	val atomic.Value
}

// Time returns a Timer where the name is chosen the first time by the caller. Don't
// use the same Thunk from different functions/methods.
func (t *Thunk) Start() Timer {
	if t.val.Load() == nil {
		t.val.Store(this.ThisN(1))
	}
	return StartNamed(t.val.Load().(string))
}

// Start returns a Timer using the calling function for the name.
func Start() (t Timer) {
	return StartNamed(this.ThisN(1))
}

// StartNamed returns a Timer that records a duration when its Done method is called.
func StartNamed(name string) Timer {
	return Timer{
		now:   nanotime(),
		state: GetState(name),
	}
}

// Timer keeps track of the state necessary to record timing info.
type Timer struct {
	now   int64
	state *State
}

// Stop records the timing info.
func (r Timer) Stop(err *error) {
	kind := ""
	if err != nil {
		kind = getKind(*err)
	}

	r.state.done(nanotime()-r.now, kind)
}

// getKind returns a string that attemps to be representative of the error.
func getKind(err error) string {
	if n, ok := err.(interface{ Name() (string, bool) }); ok {
		if name, ok := n.Name(); ok {
			return name
		}
	}

	if err != nil {
		s := err.Error()
		if i := strings.IndexByte(s, ':'); i > 0 {
			return s[:i]
		} else if strings.IndexByte(s, ' ') == -1 {
			return s
		} else {
			return "error"
		}
	}

	return ""
}
