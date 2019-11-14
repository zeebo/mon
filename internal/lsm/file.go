package lsm

import (
	"encoding/binary"
	"unsafe"
)

// we want to support 64 bit sets persisted to disk. an update to a bitset requiring
// reading the old, updating, and writing the new value is probably really expensive.
// instead, we take a page from roaring bitmaps and store bitsets as partitioned keys.
// in other words, we store `0xaa...aabb..bb` for key `k` as `k/0xaa..aa` and insert
// bb..bb into that bitmap. this should work out because we know that ids will be
// smallish, and we size the bitmaps so that they're all less than 4k so we're
// at most reading/writing a page anyway. write amplification is still a concern, but
// we care less because we have so much write i/o available in the intended application.

// lsm file
//
// 0-4  : "MLSM"
// 4-8  : uint32 level
// 8-16 : uint64 generation number
// 16-24: uint64 number of entries
// 24-32: uint64 absolute offset to entries

type header [32]byte

const headerSize = 32

func newHeader(level uint32, generation, numEntries, entryOffset uint64) (h header) {
	copy(h[0:4], "MLSM")
	binary.LittleEndian.PutUint32(h[4:8], level)
	binary.LittleEndian.PutUint64(h[8:16], generation)
	binary.LittleEndian.PutUint64(h[16:24], numEntries)
	binary.LittleEndian.PutUint64(h[24:32], entryOffset)
	return h
}

func (h header) Valid() bool         { return string(h[0:4]) == "MLSM" }
func (h header) Level() uint32       { return binary.LittleEndian.Uint32(h[4:8]) }
func (h header) Generation() uint64  { return binary.LittleEndian.Uint64(h[8:16]) }
func (h header) NumEntries() uint32  { return binary.LittleEndian.Uint32(h[16:24]) }
func (h header) EntryOffset() uint32 { return binary.LittleEndian.Uint32(h[24:32]) }

//
// lsm entry
//

type entry [32]byte

const entrySize = 32

func newEntry(kptr, vptr inlinePtr) (ent entry) {
	copy(ent[0:16], kptr[:])
	copy(ent[16:32], vptr[:])
	return ent
}

func (e *entry) Key() *inlinePtr   { return (*inlinePtr)(unsafe.Pointer(&e[0])) }
func (e *entry) Value() *inlinePtr { return (*inlinePtr)(unsafe.Pointer(&e[16])) }
