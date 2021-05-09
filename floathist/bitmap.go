package floathist

import (
	"math/bits"
	"sync/atomic"
)

type b32 [1]uint32

func (b b32) UnsafeClone() b32          { return b }
func (b b32) UnsafeUint32() uint32      { return b[0] }
func (b *b32) UnsafeSet(idx uint)       { b[0] |= 1 << (idx & 31) }
func (b *b32) UnsafeSetUint32(v uint32) { b[0] = v }

func (b *b32) Clone() b32        { return b32{atomic.LoadUint32(&b[0])} }
func (b *b32) Set(idx uint)      { atomic.AddUint32(&b[0], 1<<(idx&31)) }
func (b *b32) Has(idx uint) bool { return atomic.LoadUint32(&b[0])&(1<<(idx&31)) > 0 }

func (b *b32) Next() (idx uint32, ok bool) {
	u := b[0]
	c := u & (u - 1)
	idx = uint32(bits.Len32(u ^ c))
	b[0] = c
	return (idx - 1) % 32, u > 0
}
