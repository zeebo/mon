package buffer

import (
	"unsafe"
)

//
// custom slice support :sonic:
//

type (
	ptr  = unsafe.Pointer
	uptr = uintptr
)

type T struct {
	base ptr
	pos  uptr
	cap  uptr
}

func Of(n []byte) T {
	return T{
		base: *(*ptr)(ptr(&n)),
		pos:  0,
		cap:  uptr(cap(n)),
	}
}

func OfLen(n []byte) T {
	return T{
		base: *(*ptr)(ptr(&n)),
		pos:  0,
		cap:  uptr(len(n)),
	}
}

func (buf T) Base() ptr {
	return buf.base
}

func (buf T) Pos() uptr {
	return buf.pos
}

func (buf T) Cap() uptr {
	return buf.cap
}

func (buf T) SetPos(pos uintptr) T {
	buf.pos = pos
	return buf
}

func (buf T) Prefix() []byte {
	return *(*[]byte)(unsafe.Pointer(&buf))
}

func (buf T) At(n uptr) ptr {
	return ptr(uptr(buf.base) + buf.pos + n)
}

func (buf T) Reset() T {
	buf.pos = 0
	return buf
}

func (buf T) Front() *byte {
	return (*byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Front4() *[4]byte {
	return (*[4]byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Front8() *[8]byte {
	return (*[8]byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Front9() *[9]byte {
	return (*[9]byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Remaining() uptr {
	return buf.cap - buf.pos
}

func (buf T) Grow() T {
	if rem := buf.Remaining(); rem < 9 {
		buf.cap *= 2
		n := make([]byte, buf.cap)
		copy(n, buf.Prefix())
		buf.base = *(*ptr)(ptr(&n))
	}
	return buf
}

func (buf T) GrowN(n uintptr) T {
	if rem := buf.Remaining(); rem < n {
		buf.cap *= 2
		n := make([]byte, buf.cap)
		copy(n, buf.Prefix())
		buf.base = *(*ptr)(ptr(&n))
	}
	return buf
}

func (buf T) Index(n uintptr) *byte {
	return (*byte)(ptr(uptr(buf.base) + n))
}

func (buf T) Index4(n uintptr) *[4]byte {
	return (*[4]byte)(ptr(uptr(buf.base) + n))
}

func (buf T) Index8(n uintptr) *[8]byte {
	return (*[8]byte)(ptr(uptr(buf.base) + n))
}

func (buf T) Index9(n uintptr) *[9]byte {
	return (*[9]byte)(ptr(uptr(buf.base) + n))
}

func (buf T) Advance(n uptr) T {
	buf.pos += n
	return buf
}

func (buf T) Retreat(n uptr) T {
	buf.pos -= n
	return buf
}
