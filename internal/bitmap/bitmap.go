package bitmap

import (
	"math/bits"
	"sync/atomic"
)

//
// 64 bits
//

type B64 [1]uint64

func (b *B64) Clone() B64 {
	return B64{atomic.LoadUint64(&b[0])}
}

func (b *B64) Set(idx uint) {
	atomic.AddUint64(&b[0], 1<<(idx&63))
}

func (b *B64) Has(idx uint) bool {
	return atomic.LoadUint64(&b[0])&(1<<(idx&63)) > 0
}

func (b *B64) Next() (idx uint, ok bool) {
	u := b[0]
	c := u & (u - 1)
	idx = uint(bits.Len64(u ^ c))
	b[0] = c
	return (idx - 1) % 64, u > 0
}

//
// 128 bits
//

type B128 [2]uint64

func (b *B128) Clone() B128 {
	return B128{atomic.LoadUint64(&b[0]), atomic.LoadUint64(&b[1])}
}

func (b *B128) Set(idx uint) {
	atomic.AddUint64(&b[(idx>>6)&1], 1<<(idx&63))
}

func (b *B128) Has(idx uint) bool {
	return atomic.LoadUint64(&b[(idx>>6)&1])&(1<<(idx&63)) > 0
}

func (b *B128) Next() (idx uint, ok bool) {
	u := b[0]
	c := u & (u - 1)
	idx = uint(bits.Len64(u ^ c))
	b[0] = c

	if u > 0 {
		return (idx - 1) % 128, true
	}

	u = b[1]
	c = u & (u - 1)
	idx = 63 + uint(bits.Len64(u^c))
	b[1] = c

	return idx % 128, u > 0
}
