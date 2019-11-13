package lsm

import (
	"bufio"
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/zeebo/errs"
)

//
// inline ptr
//

type inlinePtr [16]byte

func newInlinePtrBytes(data []byte) (i inlinePtr) {
	if data == nil {
		return i
	}

	binary.LittleEndian.PutUint16(i[1:3], uint16(len(data)))
	var buf []byte

	if len(data) > 13 {
		buf = i[3:5]
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
		buf = i[3:5]
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

func (i *inlinePtr) SetOffset(offset uint64) { binary.LittleEndian.PutUint64(i[8:], offset) }

// lsm file
//
// 0-4  : "MLSM"
// 4-8  : uint32 level
// 8-16 : uint64 absolute offset to entries
// 16-24: uint64 number of entries
// 24-32: uint64 absolute offset to filter

type header [32]byte

const headerSize = 32

func newHeader(level uint32, entryOffset, numEntries, filterOffset uint64) (h header) {
	copy(h[0:4], "MLSM")
	binary.LittleEndian.PutUint32(h[4:8], level)
	binary.LittleEndian.PutUint64(h[8:16], entryOffset)
	binary.LittleEndian.PutUint64(h[16:24], numEntries)
	binary.LittleEndian.PutUint64(h[24:32], filterOffset)
	return h
}

func (h header) Valid() bool          { return string(h[0:4]) == "MLSM" }
func (h header) Level() uint32        { return binary.LittleEndian.Uint32(h[4:8]) }
func (h header) EntryOffset() uint64  { return binary.LittleEndian.Uint64(h[8:16]) }
func (h header) NumEntries() uint32   { return binary.LittleEndian.Uint32(h[16:24]) }
func (h header) FilterOffset() uint32 { return binary.LittleEndian.Uint32(h[24:32]) }

//
// lsm entry
//

type entry [32]byte

const entrySize = 32

func newEntry(key, value inlinePtr) (ent entry) {
	copy(ent[0:16], key[:])
	copy(ent[16:32], value[:])
	return ent
}

func (e *entry) Key() *inlinePtr   { return (*inlinePtr)(unsafe.Pointer(&e[0])) }
func (e *entry) Value() *inlinePtr { return (*inlinePtr)(unsafe.Pointer(&e[16])) }

//
// bufio reader wapper helper
//

type buffer struct {
	br   *bufio.Reader
	read int64
	pref bool
	err  error
}

func newBufferSize(r io.Reader, size int) buffer {
	return buffer{
		br: bufio.NewReaderSize(r, size),
	}
}

func (b *buffer) Consumed() (int64, bool) {
	return b.read, b.pref
}

func (b *buffer) Read(n int) (data []byte, ok bool) {
	if b.err != nil {
		return nil, false
	}

	data, b.err = b.br.Peek(n)
	if b.err == nil {
		return data, true
	} else if len(data) > 0 && b.err == io.EOF {
		b.err = io.ErrUnexpectedEOF
		b.pref = true
	}

	return nil, false
}

func (b *buffer) Error() error {
	return b.err
}

func (b *buffer) Consume(n int) bool {
	if b.err != nil {
		return false
	}

	var nn int
	nn, b.err = b.br.Discard(n)
	b.read += int64(nn)

	if b.err != nil {
		return false
	} else if nn != n {
		b.err = errs.New("invalid discard")
		return false
	}

	return true
}

//
// no-op flusher
//

type noopFlusher struct{ io.Writer }

func (noopFlusher) Flush() error { return nil }
