package lsm

import (
	"encoding/binary"
)

const (
	inlinePtr_Null    = 0
	inlinePtr_Pointer = 1
	inlinePtr_Inline  = 2
)

type inlinePtr [16]byte

func newInlinePtrBytes(data []byte) (i inlinePtr) {
	if data != nil {
		binary.LittleEndian.PutUint16(i[1:3], uint16(len(data)))
		var buf []byte

		if len(data) > 13 {
			buf = i[3:11]
			i[0] = 1
		} else {
			buf = i[3:16]
			i[0] = 2
		}

		copy(buf, data)
	}

	return i
}

func newInlinePtrString(data string) (i inlinePtr) {
	binary.LittleEndian.PutUint16(i[1:3], uint16(len(data)))
	var buf []byte

	if len(data) > 13 {
		buf = i[3:11]
		i[0] = 1
	} else {
		buf = i[3:16]
		i[0] = 2
	}

	copy(buf, data)
	return i
}

func (i inlinePtr) Null() bool    { return i[0] == inlinePtr_Null }
func (i inlinePtr) Pointer() bool { return i[0] == inlinePtr_Pointer }
func (i inlinePtr) Inline() bool  { return i[0] == inlinePtr_Inline }

func (i inlinePtr) Length() int    { return int(binary.LittleEndian.Uint16(i[1:3])) }
func (i inlinePtr) Prefix() uint64 { return binary.BigEndian.Uint64(i[3:]) }
func (i inlinePtr) Offset() uint64 { return binary.BigEndian.Uint64(i[8:]) & 0x000000FFFFFFFFFF }

func (i inlinePtr) InlineData() []byte {
	end := 3 + i.Length()
	if i[0] == inlinePtr_Inline && end >= 3 && end < len(i) {
		return i[3:end]
	}
	return nil
}

func (i *inlinePtr) SetOffset(offset uint64) {
	i[11] = byte(offset >> 32)
	i[12] = byte(offset >> 24)
	i[13] = byte(offset >> 16)
	i[14] = byte(offset >> 8)
	i[15] = byte(offset)
}

type inlinePtrReader interface {
	AppendPointer(ptr inlinePtr, buf []byte) ([]byte, error)
}
