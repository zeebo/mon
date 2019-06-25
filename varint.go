package mon

import (
	"encoding/binary"
	"math/bits"
	"unsafe"
)

//
// custom slice support
//

const buffMax = 1 << 30

type (
	ptr  = unsafe.Pointer
	uptr = uintptr
)

var le = binary.LittleEndian

type buffer struct {
	base ptr
	pos  uptr
	cap  uptr
}

func bufferOf(n []byte) buffer {
	return buffer{
		base: *(*ptr)(ptr(&n)),
		pos:  0,
		cap:  uptr(cap(n)),
	}
}

func (b buffer) prefix() []byte {
	return (*[buffMax]byte)(b.base)[: b.pos%buffMax : b.cap%buffMax]
}

func (b buffer) suffix() []byte {
	return (*[buffMax]byte)(b.base)[b.pos : b.cap%buffMax : b.cap%buffMax]
}

func (b buffer) at(n uptr) ptr {
	return ptr(uptr(b.base) + b.pos + n)
}

func (b buffer) reset() buffer {
	b.pos = 0
	return b
}

//
// varint support
//

var masks = [8]uint8{0, 0, 1, 3, 7, 15, 0, 0}

func varintAppend(buf buffer, n uint32) buffer {
	nbits := uint8(bits.Len32(n | 1))
	nbytes := (nbits + 6) / 7 % 8
	mask := masks[nbytes]
	val := uint64(n)<<nbytes | uint64(mask)

	// ensure we always have at least 8 bytes so we can put easily
	if rem := buf.cap - buf.pos; rem < 8 {
		buf.cap *= 2
		n := make([]byte, buf.cap)
		copy(n, buf.prefix())
		buf.base = *(*ptr)(ptr(&n))
	}

	loc := (*[8]byte)(buf.at(0))
	le.PutUint64(loc[:], val)
	buf.pos += uintptr(nbytes)

	return buf
}

func varintConsume(buf buffer) (uint32, buffer) {
	rem := buf.cap - buf.pos
	if rem == 0 {
		return 0, buffer{}
	}

	// if we have at least 8 bytes remaining, we can read extra
	if rem >= 8 {
		out := le.Uint64((*[8]byte)(buf.at(0))[:])
		nbytes := uptr(bits.TrailingZeros8(^uint8(out))+1) % 8
		out >>= nbytes
		buf.pos += nbytes
		return uint32(out), buf
	}

	// slow path: can't create or use any pointers past the end of the buf
	out := uint32(*(*byte)(buf.at(0)))
	nbytes := uptr(bits.TrailingZeros8(^uint8(out))+1) % 8
	out >>= nbytes

	switch nbytes {
	case 5:
		out |= le.Uint32((*[4]byte)(buf.at(1))[:]) << 3
	case 4:
		out |= uint32(le.Uint16((*[2]byte)(buf.at(1))[:])) << 4
		out |= uint32(*(*byte)(buf.at(3))) << 20
	case 3:
		out |= uint32(le.Uint16((*[2]byte)(buf.at(1))[:])) << 5
	case 2:
		out |= uint32(*(*byte)(buf.at(1))) << 6
	}

	buf.pos += nbytes
	return out, buf
}
