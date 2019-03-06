package mon

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// State keeps track of all of the timer information for some calls.
type State struct {
	current int64
	his     Histogram
}

var ( // states is a map[string]*tracker
	states unsafe.Pointer
	mu     sync.Mutex
)

// initialize histograms so that we don't have to do it lazily
func init() { storeState(make(map[string]*State)) }

// storeHistogram overwrites the histogram map
func storeState(hs map[string]*State) {
	atomic.StorePointer(&states, unsafe.Pointer(&hs))
}

// loadState atomically loads the current histogram map
func loadState() map[string]*State {
	return *(*map[string]*State)(atomic.LoadPointer(&states))
}

// GetState returns the current state for some name.
func GetState(name string) *State {
	return loadState()[name]
}

// newState allocates a state for name and stores it in the
// global set in a race free way.
func newState(name string) *State {
	mu.Lock()

	states := loadState()
	state, ok := states[name]
	if ok {
		mu.Unlock()
		return state
	}

	next := make(map[string]*State, len(states)+1)
	for name, state := range states {
		next[name] = state
	}

	state = new(State)
	next[name] = state
	storeState(next)

	mu.Unlock()
	return state
}

// start informs the state that a task is starting.
func (s *State) start() { atomic.AddInt64(&s.current, 1) }

// done informs the State that a task has completed in the given
// amount of nanoseconds.
func (s *State) done(v int64) {
	atomic.AddInt64(&s.current, -1)
	s.his.Observe(v)
}

// Histogram returns the Histogram associated with the state.
func (s *State) Histogram() *Histogram { return &s.his }

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
