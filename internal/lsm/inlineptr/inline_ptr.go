package inlineptr

import (
	"encoding/binary"
	"fmt"
	"strings"
)

const (
	Null    = 0
	Pointer = 1
	Inline  = 2
)

type T [Size]byte

const Size = 16

func Bytes(data []byte) (i T) {
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

func String(data string) (i T) {
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

func (i T) Null() bool    { return i[0] == Null }
func (i T) Pointer() bool { return i[0] == Pointer }
func (i T) Inline() bool  { return i[0] == Inline }

func (i T) Length() int    { return int(binary.LittleEndian.Uint16(i[1:3])) }
func (i T) Prefix() uint64 { return binary.BigEndian.Uint64(i[3:]) }
func (i T) Offset() uint64 { return binary.BigEndian.Uint64(i[8:]) & 0x000000FFFFFFFFFF }

func (i T) InlineData() []byte {
	end := 3 + i.Length()
	if i[0] == Inline && end >= 3 && end < len(i) {
		return i[3:end]
	}
	return nil
}

func (i *T) SetOffset(offset uint64) {
	i[11] = byte(offset >> 32)
	i[12] = byte(offset >> 24)
	i[13] = byte(offset >> 16)
	i[14] = byte(offset >> 8)
	i[15] = byte(offset)
}

func (i T) String() string {
	out := "N"
	if i.Inline() {
		out = fmt.Sprintf("I:%02d:%x", i.Length(), i.InlineData())
	} else if i.Pointer() {
		out = fmt.Sprintf("P:%02d:%x:%d", i.Length(), i.Prefix(), i.Offset())
	}
	out += strings.Repeat(" ", 32-len(out))
	return out
}
