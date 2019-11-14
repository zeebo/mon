package lsm

import (
	"encoding/binary"

	"github.com/zeebo/errs"
)

const (
	inlinePtr_Null    = 0
	inlinePtr_Pointer = 1
	inlinePtr_Inline  = 2
)

type inlinePtr [16]byte

func newInlinePtrBytes(data []byte) (i inlinePtr) {
	if data == nil {
		return i
	}

	binary.LittleEndian.PutUint16(i[1:3], uint16(len(data)))
	var buf []byte

	if len(data) > 13 {
		buf = i[3:8]
		i[0] = 1
	} else {
		buf = i[3:16]
		i[0] = 2
	}

	copy(buf, data)
	return i
}

func newInlinePtrString(data string) (i inlinePtr) {
	binary.LittleEndian.PutUint16(i[1:3], uint16(len(data)))
	var buf []byte

	if len(data) > 13 {
		buf = i[3:8]
		i[0] = 1
	} else {
		buf = i[3:16]
		i[0] = 2
	}

	copy(buf, data)
	return i
}

func (i inlinePtr) Null() bool    { return i[0] == 0 }
func (i inlinePtr) Pointer() bool { return i[0] == 1 }
func (i inlinePtr) Inline() bool  { return i[0] == 2 }

func (i inlinePtr) Length() int    { return int(binary.LittleEndian.Uint16(i[1:3])) }
func (i inlinePtr) Prefix() uint64 { return binary.BigEndian.Uint64(i[:]) & 0x000000FFFFFFFFFF }
func (i inlinePtr) Offset() uint64 { return binary.LittleEndian.Uint64(i[8:]) }

func (i inlinePtr) InlineData() []byte {
	end := 3 + i.Length()
	if end >= 3 && end < len(i) {
		return i[3:end]
	}
	return nil
}

func (i *inlinePtr) SetOffset(offset uint64) { binary.LittleEndian.PutUint64(i[8:], offset) }

type inlinePtrReader interface {
	ReadPointer(ptr inlinePtr) ([]byte, error)
}

func readInlinePointer(r inlinePtrReader, ptr inlinePtr) ([]byte, error) {
	switch ptr[0] {
	case inlinePtr_Null:
		return nil, nil

	case inlinePtr_Pointer:
		return r.ReadPointer(ptr)

	case inlinePtr_Inline:
		if data := ptr.InlineData(); data != nil {
			return data, nil
		}
	}

	return nil, errs.New("invalid inline pointer")
}
