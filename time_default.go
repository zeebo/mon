// +build !instrumented

package mon

// Times calls the callback with all of the histograms that have been captured.
func Times(func(string, *Histogram) bool) {}

// Thunk is a type that allows one to get the benefits of Time without having to
// compute the caller every time it's called. Zero values are valid.
type Thunk struct{}

// Time returns a Timer where the name is chosen the first time by the caller. Don't
// use the same Thunk from different functions/methods.
func (t *Thunk) Start() Timer { return Timer{} }

// Time returns a Timer using the calling function for the name.
func Start() Timer { return Timer{} }

// TimeNamed returns a Timer that records a duration when its Done method is called.
func StartNamed(name string) Timer { return Timer{} }

// Timer keeps track of the state necessary to record timing info.
type Timer struct{}

// Stop records the timing info.
func (Timer) Stop() {}
