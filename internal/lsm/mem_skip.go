package lsm

import (
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/pcg"
)

const (
	skipMemLevels = 5
	skipMemRatio  = 4
	skipMemSize   = 1 << (skipMemLevels * skipMemRatio)
)

type skipMemEntry struct {
	kptr inlinePtr
	val  uint32
}

type skipMemPtr struct {
	prefix uint64
	ptrs   [skipMemLevels]uint32
}

type skipMem struct {
	data  []byte
	vptrs []inlinePtr
	cap   uint64
	rng   pcg.T

	len  uint32 // number of inserted entries
	ents *[skipMemSize]skipMemEntry
	ptrs *[skipMemSize]skipMemPtr
}

func (m *skipMem) init(cap uint64) {
	m.data = make([]byte, 0, cap)
	m.vptrs = make([]inlinePtr, 0, cap/entrySize)
	m.cap = cap

	m.ents = new([skipMemSize]skipMemEntry)
	m.ptrs = new([skipMemSize]skipMemPtr)
}

func (m *skipMem) Keys() uint32 { return uint32(m.len) }
func (m *skipMem) Cap() uint64  { return m.cap }
func (m *skipMem) Len() uint64  { return entrySize*(uint64(m.len)) + uint64(len(m.data)) }

func (m *skipMem) reset() {
	m.data = m.data[:0]
	m.vptrs = m.vptrs[:0]

	m.len = 0
	m.ptrs[0].ptrs = [skipMemLevels]uint32{}
}

func (m *skipMem) SetBytes(key, value []byte) bool {
	return m.SetString(*(*string)(unsafe.Pointer(&key)), value)
}

func (m *skipMem) SetString(key string, value []byte) bool {
	vptr := newInlinePtrBytes(value)
	if vptr.Pointer() {
		vptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, value...)
	}

	kptr := newInlinePtrString(key)
	if kptr.Pointer() {
		kptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, key...)
	}

	if m.set(kptr, key, uint32(len(m.vptrs))) {
		m.vptrs = append(m.vptrs, vptr)
		return m.Len() < m.Cap()
	}

	return false
}

func (m *skipMem) set(kptr inlinePtr, key string, val uint32) bool {
	prefix := kptr.Prefix()
	mlen := m.len
	ents := m.ents
	ptrs := m.ptrs

	prevs := [len(m.ptrs[0].ptrs)]*skipMemPtr{}
	ptr := &ptrs[0]

	for i := len(prevs) - 1; i >= 0; i-- {
		for {
			nid := atomic.LoadUint32(&ptr.ptrs[i])
			nprefix := ptrs[nid].prefix

			if nid == 0 || nid > mlen || nprefix > prefix {
				break
			}

			if prefix == nprefix {
				nkptr := &ents[nid].kptr

				var nkey []byte
				if nkptr.Pointer() {
					begin := nkptr.Offset()
					end := begin + uint64(nkptr.Length())
					nkey = m.data[begin:end]
				} else {
					nkey = nkptr.InlineData()
				}

				if key <= string(nkey) {
					break
				}
			}

			ptr = &ptrs[nid]
		}

		prevs[i] = ptr
	}

	if mlen > 0 {
		id := atomic.LoadUint32(&ptr.ptrs[0])

		if id != 0 && prefix == ptrs[id].prefix {
			nkptr := &ents[id].kptr

			var nkey []byte
			if nkptr.Pointer() {
				begin := nkptr.Offset()
				end := begin + uint64(nkptr.Length())
				nkey = m.data[begin:end]
			} else {
				nkey = nkptr.InlineData()
			}

			if string(nkey) == key {
				ents[id].val = val
				return true
			}
		}
	}

	id := atomic.AddUint32(&m.len, 1)
	if id >= skipMemSize {
		return false
	}

	ent := &m.ents[id]
	ent.kptr = kptr
	ent.val = val

	level := bits.TrailingZeros32(m.rng.Uint32())/skipMemRatio + 1
	if level > skipMemLevels {
		level = skipMemLevels
	}

	ptr = &ptrs[id]
	ptr.prefix = prefix
	for i := 0; i < level; i++ {
		pptr := prevs[i]
		for {
			next := atomic.LoadUint32(&pptr.ptrs[i])

			ptr.ptrs[i] = next
			if atomic.CompareAndSwapUint32(&pptr.ptrs[i], next, id) {
				break
			}

			panic("TODO")
		}
	}

	m.len++

	return true
}

func (m *skipMem) iter() skipMemIterator {
	return skipMemIterator{m: m}
}

func (m *skipMem) iterGen() interface{ Next() bool } {
	it := m.iter()
	return &it
}

type skipMemIterator struct {
	m  *skipMem
	id uint32

	ent entry
	key []byte
	val []byte
}

func (i *skipMemIterator) Next() bool {
	i.id = atomic.LoadUint32(&i.m.ptrs[i.id].ptrs[0])
	if i.id == 0 {
		return false
	}

	sent := &i.m.ents[i.id]
	i.ent = newEntry(sent.kptr, i.m.vptrs[sent.val])

	switch kptr := i.ent.Key(); kptr[0] {
	case inlinePtr_Inline:
		i.key = append(i.key[:0], kptr.InlineData()...)
	case inlinePtr_Pointer:
		begin := kptr.Offset()
		end := begin + uint64(kptr.Length())
		i.key = i.m.data[begin:end]
	}

	switch vptr := i.ent.Value(); vptr[0] {
	case inlinePtr_Inline:
		i.val = append(i.val[:0], vptr.InlineData()...)
	case inlinePtr_Pointer:
		begin := vptr.Offset()
		end := begin + uint64(vptr.Length())
		i.val = i.m.data[begin:end]
	}

	return true
}

func (i *skipMemIterator) Entry() entry { return i.ent }

func (i *skipMemIterator) Key() []byte {
	if i.ent.Key().Null() {
		return nil
	}
	return i.key
}

func (i *skipMemIterator) Value() []byte {
	if i.ent.Value().Null() {
		return nil
	}
	return i.val
}

func (i *skipMemIterator) Err() error { return nil }
