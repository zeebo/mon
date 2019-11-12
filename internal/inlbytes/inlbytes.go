package inlbytes

import (
	"math/bits"
	"reflect"
	"unsafe"
)

const MaxInlined = bits.UintSize/4 - 1

type T struct {
	// declared this way so that we can inline Uint56 LOL
	Data *[MaxInlined]byte
	Len  int8
	Rem  [MaxInlined]byte
}

func FromBytes(data []byte) T {
	if data == nil {
		return T{}
	} else if len(data) > MaxInlined {
		return *(*T)(unsafe.Pointer(&data))
	} else {
		return inline(data)
	}
}

func FromString(data string) T {
	if data := stringToBytes(data); len(data) > MaxInlined {
		return *(*T)(unsafe.Pointer(&data))
	} else {
		return inline(data)
	}
}

func inline(data []byte) T {
	var t T
	t.Len = int8(copy(*(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&t.Rem)),
		Len:  len(data),
		Cap:  len(data),
	})), data)) + 1
	return t
}

func (t T) Length() int {
	if t.Data != nil {
		return *(*int)(unsafe.Pointer(&t.Len))
	} else if t.Len == 0 {
		return 0
	} else {
		return int(t.Len) - 1
	}
}

func (t T) Bytes() []byte {
	if t.Data != nil {
		return *(*[]byte)(unsafe.Pointer(&t))
	} else if t.Len == 0 {
		return nil
	} else {
		return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
			Data: uintptr(unsafe.Pointer(&t.Rem)),
			Len:  int(t.Len) - 1,
			Cap:  int(t.Len) - 1,
		}))
	}
}

func (t T) String() string {
	if t.Data != nil {
		return *(*string)(unsafe.Pointer(&t))
	} else if t.Len == 0 {
		return ""
	} else {
		return *(*string)(unsafe.Pointer(&reflect.StringHeader{
			Data: uintptr(unsafe.Pointer(&t.Rem)),
			Len:  int(t.Len) - 1,
		}))
	}
}

func (t T) Uint56() uint64 {
	buf := &t.Rem
	if t.Data != nil {
		buf = t.Data
	}
	return (uint64(buf[7]) |
		uint64(buf[6])<<8 |
		uint64(buf[5])<<16 |
		uint64(buf[4])<<24 |
		uint64(buf[3])<<32 |
		uint64(buf[2])<<40 |
		uint64(buf[1])<<48 |
		uint64(buf[0])<<56) >> 8
}

func stringToBytes(x string) []byte {
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: *(*uintptr)(unsafe.Pointer(&x)),
		Len:  len(x),
		Cap:  len(x),
	}))
}
