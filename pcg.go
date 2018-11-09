package mon

import (
	"math/bits"
)

// pcg is a pcg generator. The zero value is invalid.
type pcg struct {
	State uint64
	Inc   uint64
}

const mul = 6364136223846793005

// newPCG constructs a pcg with the given state and inc.
func newPCG(state, inc uint64) pcg {
	// this code is equiv to initializing a pcg with a 0 state and the updated
	// inc and running
	//
	//    p.Uint32()
	//    p.State += state
	//    p.Uint32()
	//
	// to get the generator started
	inc = inc<<1 | 1
	return pcg{
		State: (inc+state)*mul + inc,
		Inc:   inc,
	}
}

// Uint32 returns a random uint32.
func (p *pcg) Uint32() uint32 {
	// update the state (LCG step)
	oldstate := p.State
	p.State = oldstate*mul + p.Inc

	// apply the output permutation to the old state
	// NOTE: this should be a right rotate but i can't coerce the compiler into
	// doing it. since any rotate should be sufficient for the output compression
	// function, this ought to be fine, and is significantly faster.

	xorshift := uint32(((oldstate >> 18) ^ oldstate) >> 27)
	return bits.RotateLeft32(xorshift, int(oldstate>>59))
}

// Intn returns an int uniformly in [0, n)
func (p *pcg) Intn(n int) int {
	return fastMod(p.Uint32(), n)
}

// Float64 returns a float uniformly in [0, 1)
func (p *pcg) Float64() float64 {
	u53 := uint64(p.Uint32()<<21) | uint64(p.Uint32()>>11)
	return float64(u53) / (1 << 53)
}

// fastMod computes n % m assuming that n is a random number in the full
// uint32 range.
func fastMod(n uint32, m int) int {
	return int((uint64(n) * uint64(m)) >> 32)
}
