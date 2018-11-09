// +build instrumented

package mon

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

//go:linkname nanotime runtime.nanotime
func nanotime() (mono int64)

var ( // histograms is a map[string]*Histogram
	histograms unsafe.Pointer
	mu         sync.Mutex
)

// initialize histograms so that we don't have to do it lazily
func init() { storeHistograms(make(map[string]*Histogram)) }

// storeHistogram overwrites the histogram map
func storeHistograms(hs map[string]*Histogram) {
	atomic.StorePointer(&histograms, unsafe.Pointer(&hs))
}

// loadHistograms atomically loads the current histogram map
func loadHistograms() map[string]*Histogram {
	return *(*map[string]*Histogram)(atomic.LoadPointer(&histograms))
}

// Times calls the callback with all of the histograms that have been captured.
func Times(cb func(string, *Histogram) bool) {
	for name, his := range loadHistograms() {
		if !cb(name, his) {
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
	// grab the appropriate histogram structure
	his, ok := loadHistograms()[name]
	if !ok {
		// if we missed, we have a slow case. pull it into a function
		// so that the code for it is located elsewhere in the binary
		his = newHistogram(name)
	}

	his.start()

	// record the time and return a struct to run it
	return Timer{
		now: nanotime(),
		his: his,
	}
}

// newHistogram allocates a histogram for name and stores it in the
// global set in a race free way.
func newHistogram(name string) *Histogram {
	mu.Lock()

	// attempt again with the mutex to see if we lost a race
	hs := loadHistograms()
	h, ok := hs[name]
	if ok {
		return h
	}

	// create the copy
	next := make(map[string]*Histogram, len(hs)+1)
	for key, val := range hs {
		next[key] = val
	}

	// insert and store our updated map
	h = new(Histogram)
	next[name] = h
	storeHistograms(next)

	mu.Unlock()
	return h
}

// Timer keeps track of the state necessary to record timing info.
type Timer struct {
	now int64
	his *Histogram
}

// Stop records the timing info.
func (r Timer) Stop() { r.his.done(nanotime() - r.now) }
