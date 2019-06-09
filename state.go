package mon

import (
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/mon/internal/lfht"
)

func newState() unsafe.Pointer   { return unsafe.Pointer(new(State)) }
func newCounter() unsafe.Pointer { return unsafe.Pointer(new(int64)) }

// State keeps track of all of the timer information for some calls.
type State struct {
	current int64
	errors  lfht.Table
	his     Histogram
}

// states maps names to State pointers.
var states lfht.Table

// GetState returns the current state for some name, allocating a new one if necessary.
func GetState(name string) *State { return (*State)(states.Upsert(name, newState)) }

// LookupState returns the current state for some name, returning nil if none exists.
func LookupState(name string) *State { return (*State)(states.Lookup(name)) }

// start informs the state that a task is starting.
func (s *State) start() { atomic.AddInt64(&s.current, 1) }

// done informs the State that a task has completed in the given
// amount of nanoseconds.
func (s *State) done(v int64, kind string) {
	atomic.AddInt64(&s.current, -1)
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

// Current returns the number of active calls.
func (s *State) Current() int64 { return atomic.LoadInt64(&s.current) }

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
