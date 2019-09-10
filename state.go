package mon

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/mon/internal/lfht"
	"github.com/zeebo/swaparoo"
)

var (
	statesMu sync.Mutex       // protects concurrent Collect calls.
	states   [2]lfht.Table    // states maps names to State pointers.
	tracker  swaparoo.Tracker // keeps track of which state is valid.
)

func newState() unsafe.Pointer   { return unsafe.Pointer(new(State)) }
func newCounter() unsafe.Pointer { return unsafe.Pointer(new(int64)) }

// State keeps track of all of the timer information for some calls.
type State struct {
	errors lfht.Table
	his    Histogram
}

// Times calls the callback with all of the histograms that have been captured.
func Times(cb func(string, *State) bool) {
	token := tracker.Acquire()
	for iter := states[token.Gen()%2].Iterator(); iter.Next(); {
		if !cb(iter.Key(), (*State)(iter.Value())) {
			goto done
		}
	}
done:
	token.Release()
}

// Collect consumes all of the histograms that have been captures and calls
// the callback for each one.
func Collect(cb func(string, *State) bool) {
	statesMu.Lock()
	gen := tracker.Increment().Wait()
	for iter := states[gen%2].Iterator(); iter.Next(); {
		if !cb(iter.Key(), (*State)(iter.Value())) {
			goto done
		}
	}
done:
	states[gen%2] = lfht.Table{}
	statesMu.Unlock()
}

// GetState returns the current state for some name, allocating a new one if necessary.
func GetState(name string) *State {
	token := tracker.Acquire()
	state := (*State)(states[token.Gen()%2].Upsert(name, newState))
	token.Release()
	return state
}

// LookupState returns the current state for some name, returning nil if none exists.
func LookupState(name string) *State {
	token := tracker.Acquire()
	state := (*State)(states[token.Gen()%2].Lookup(name))
	token.Release()
	return state
}

// done informs the State that a task has completed in the given
// amount of nanoseconds.
func (s *State) done(v int64, kind string) {
	s.his.Observe(v)
	if kind != "" {
		counter := (*int64)(s.errors.Upsert(kind, newCounter))
		atomic.AddInt64(counter, 1)
	}
}

// Histogram returns the Histogram associated with the state.
func (s *State) Histogram() *Histogram { return &s.his }

// Errors returns a tree of error counters. Be sure to use atomic.LoadInt64 on the results.
func (s *State) Errors() *lfht.Table { return &s.errors }

// Total returns the number of completed calls.
func (s *State) Total() int64 { return s.his.Total() }

// Quantile returns an estimation of the qth quantile in [0, 1].
func (s *State) Quantile(q float64) int64 { return s.his.Quantile(q) }

// Sum returns an estimation of the sum.
func (s *State) Sum() float64 { return s.his.Sum() }

// Average returns an estimation of the sum and average.
func (s *State) Average() (float64, float64) { return s.his.Average() }

// Variance returns an estimation of the sum, average and variance.
func (s *State) Variance() (float64, float64, float64) { return s.his.Variance() }
