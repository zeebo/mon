package mon

import (
	"encoding/binary"
	"math/bits"

	"github.com/zeebo/mon/internal/buffer"
)

//
// varint support
//

var masks = [8]uint8{0, 0, 1, 3, 7, 15, 0, 0}

// varintStats returns
func varintStats(val uint32) (uint8, uint64) {
	nbits := uint8(bits.Len32(val | 1))
	nbytes := (nbits + 6) / 7 % 8
	mask := masks[nbytes]
	return nbytes, uint64(val)<<nbytes | uint64(mask)
}

func varintConsume(buf buffer.T) (uint32, buffer.T) {
	var le = binary.LittleEndian

	rem := buf.Remaining()
	if rem == 0 {
		return 0, buffer.T{}
	}

	// if we have at least 8 bytes remaining, we can read extra
	if rem >= 8 {
		out := le.Uint64(buf.Front()[:])
		nbytes := uint8(bits.TrailingZeros8(^uint8(out))+1) % 8
		out >>= nbytes
		return uint32(out), buf.Advance(uintptr(nbytes))
	}

	// slow path: can't create or use any pointers past the end of the buf
	out := uint32(*(*byte)(buf.At(0)))
	nbytes := uint8(bits.TrailingZeros8(^uint8(out))+1) % 8
	out >>= nbytes

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

	return out, buf.Advance(uintptr(nbytes))
}
