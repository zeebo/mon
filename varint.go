package mon

import (
	"encoding/binary"
	"math/bits"

	"github.com/zeebo/mon/internal/buffer"
)

//
// varint support
//

func varintStats(val uint32) (nbytes uint8, enc uint64) {
	switch {
	case val < 1<<7:
		return 1, uint64(val)<<1 | 0
	case val < 1<<14:
		return 2, uint64(val)<<2 | 1
	case val < 1<<21:
		return 3, uint64(val)<<3 | 3
	case val < 1<<28:
		return 4, uint64(val)<<4 | 7
	default:
		return 5, uint64(val)<<5 | 15
	}
}

//go:noinline
func fastVarintConsume(val uint64) (nbytes uint8, dec uint32) {
	nbytes = uint8(bits.TrailingZeros8(^uint8(val)) + 1)
	val <<= (64 - 8*nbytes) % 64
	val >>= (64 - 7*nbytes) % 64
	return nbytes, uint32(val)
}

func safeVarintConsume(buf buffer.T) (uint32, buffer.T, bool) {
	le := binary.LittleEndian

	rem := buf.Remaining()
	if rem == 0 {
		return 0, buf, false
	}

	// slow path: can't create or use any pointers past the end of the buf
	out := uint32(*(*byte)(buf.At(0)))
	nbytes := uint8(bits.TrailingZeros8(^uint8(out))+1) % 8
	out >>= nbytes

	if uintptr(nbytes) > rem {
		return 0, buf, false
	}

	switch nbytes {
	case 5:
		out |= le.Uint32((*[4]byte)(buf.At(1))[:]) << 3
	case 4:
		out |= uint32(le.Uint16((*[2]byte)(buf.At(1))[:])) << 4
		out |= uint32(*(*byte)(buf.At(3))) << 20
	case 3:
		out |= uint32(le.Uint16((*[2]byte)(buf.At(1))[:])) << 5
	case 2:
		out |= uint32(*(*byte)(buf.At(1))) << 6
	}

	return out, buf.Advance(uintptr(nbytes)), true
}
