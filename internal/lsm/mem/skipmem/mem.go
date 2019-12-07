package skipmem

import (
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
	"github.com/zeebo/mon/internal/lsm/iterator"
	"github.com/zeebo/pcg"
)

const (
	skipMemLevels = 10
	skipMemRatio  = 2
	skipMemSize   = 1 << (skipMemLevels * skipMemRatio)
)

type skipMemEntry struct {
	kptr inlineptr.T
	val  uint32
}

type skipMemPtr struct {
	prefix uint64
	ptrs   [skipMemLevels]uint32
}

//
//
//

type T struct {
	data  []byte
	vptrs []inlineptr.T
	cap   uint64
	rng   pcg.T

	len  uint32 // number of inserted entries
	ents *[skipMemSize]skipMemEntry
	ptrs *[skipMemSize]skipMemPtr
}

func (m *T) Init(cap uint64) {
	m.data = make([]byte, 0, cap)
	m.vptrs = make([]inlineptr.T, 0, cap/entry.Size)
	m.cap = cap

	m.ents = new([skipMemSize]skipMemEntry)
	m.ptrs = new([skipMemSize]skipMemPtr)
}

func (m *T) Iter() Iterator {
	return Iterator{m: m}
}

func (m *T) Iters() []iterator.T {
	it := m.Iter()
	return []iterator.T{&it}
}

func (m *T) Reset() {
	m.data = m.data[:0]
	m.vptrs = m.vptrs[:0]

	m.len = 0
	m.ptrs[0].ptrs = [skipMemLevels]uint32{}
}

func (m *T) Keys() uint32 { return uint32(m.len) }
func (m *T) Cap() uint64  { return m.cap }
func (m *T) Len() uint64  { return entry.Size*(uint64(m.len)) + uint64(len(m.data)) }

func (m *T) SetBytes(key, value []byte) bool {
	return m.SetString(*(*string)(unsafe.Pointer(&key)), value)
}

func (m *T) SetString(key string, value []byte) bool {
	vptr := inlineptr.Bytes(value)
	if vptr.Pointer() {
		vptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, value...)
	}

	kptr := inlineptr.String(key)
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

func (m *T) set(kptr inlineptr.T, key string, val uint32) bool {
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

	level := bits.TrailingZeros32(m.rng.Uint32()|(skipMemSize/2)) / skipMemRatio
	atomic.AddUint64(&Buckets[level], 1)

	ptr = &ptrs[id]
	ptr.prefix = prefix
	for i := 0; i <= level; i++ {
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

	return true
}

var Buckets [skipMemLevels]uint64
