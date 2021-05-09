package floathist

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/zeebo/mon/internal/buffer"
)

//
// varint support
//

func varintAppend(dst *[9]byte, val uint64) (nbytes uintptr) {
	nbytes = 575*uintptr(bits.Len64(val))/4096 + 1

	if nbytes < 9 {
		enc := val<<nbytes + 1<<((nbytes-1)&63) - 1
		*(*uint64)(ptr(&dst[0])) = enc // annoying
		return
	}

	dst[0] = 0xff
	*(*uint64)(ptr(&dst[1])) = val // annoying
	return
}

func fastVarintConsume(src *[9]byte) (nbytes uintptr, dec uint64) {
	nbytes = uintptr(bits.TrailingZeros8(^src[0])) + 1

	if nbytes < 9 {
		dec = *(*uint64)(ptr(&src[0])) >> nbytes // annoying
		dec &= 1<<((8*nbytes-nbytes)&63) - 1
		return
	}

	dec = *(*uint64)(ptr(&src[1])) // annoying
	return
}

func safeVarintConsume(buf buffer.T) (uint64, buffer.T, bool) {
	le := binary.LittleEndian

	rem := buf.Remaining()
	if rem == 0 {
		return 0, buf, false
	}

	// slow path: can't create or use any pointers past the end of the buf
	out := uint64(*buf.Front())
	nbytes := uint8(bits.TrailingZeros8(^uint8(out)) + 1)
	out >>= nbytes

	if uintptr(nbytes) > rem {
		return 0, buf, false
	}

	switch nbytes {
	case 9:
		out |= le.Uint64((*[8]byte)(buf.At(1))[:])
	case 8:
		out |= uint64(le.Uint32((*[4]byte)(buf.At(1))[:]))
		out |= uint64(le.Uint32((*[4]byte)(buf.At(4))[:])) << 24
	case 7:
		out |= uint64(le.Uint32((*[4]byte)(buf.At(1))[:])) << 1
		out |= uint64(le.Uint16((*[2]byte)(buf.At(5))[:])) << 33
	case 6:
		out |= uint64(le.Uint32((*[4]byte)(buf.At(1))[:])) << 2
		out |= uint64(*(*byte)(buf.At(5))) << 34
	case 5:
		out |= uint64(le.Uint32((*[4]byte)(buf.At(1))[:])) << 3
	case 4:
		out |= uint64(le.Uint16((*[2]byte)(buf.At(1))[:])) << 4
		out |= uint64(*(*byte)(buf.At(3))) << 20
	case 3:
		out |= uint64(le.Uint16((*[2]byte)(buf.At(1))[:])) << 5
	case 2:
		out |= uint64(*(*byte)(buf.At(1))) << 6
	}

	return out, buf.Advance(uintptr(nbytes)), true
}

//
// we use direct uint64 writes because the inliner hates binary.LittleEndian :(
//

func init() {
	var b1, b2 [9]byte
	binary.LittleEndian.PutUint64(b1[1:9], 0x0102030405060708)
	*(*uint64)(ptr(&b2[1])) = 0x0102030405060708
	if b1 != b2 {
		panic(fmt.Sprintf("not on little-endian machine: %x != %x", b1, b2))
	}
}
