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

//
//
//

type T struct {
	data   []byte
	ents   []entry.T
	cap    uint64
	rng    pcg.T
	chunks [skipMemLevels]chunk
}

func (m *T) Init(cap uint64) {
	m.data = make([]byte, 0, cap)
	m.ents = make([]entry.T, 0, cap/entry.Size)
	m.cap = cap
	for i := skipMemLevels - 1; i > 0; i-- {
		m.chunks[i].down = &m.chunks[i-1]
	}
}

func (m *T) Iter() Iterator {
	return Iterator{
		m:   m,
		cur: m.chunks[0].cursor(),
	}
}

func (m *T) Iters() []iterator.T {
	it := m.Iter()
	return []iterator.T{&it}
}

func (m *T) Reset() {
	m.data = m.data[:0]
	m.ents = m.ents[:0]
	for i := range m.chunks {
		m.chunks[i].reset()
		if i > 0 {
			m.chunks[i].down = &m.chunks[i-1]
		}
	}
}

func (m *T) Keys() uint32 { return uint32(len(m.ents)) }
func (m *T) Cap() uint64  { return m.cap }
func (m *T) Len() uint64  { return entry.Size*(uint64(len(m.ents))) + uint64(len(m.data)) }

func (m *T) SetBytes(key, value []byte) bool {
	return m.SetString(*(*string)(unsafe.Pointer(&key)), value)
}

func (m *T) SetString(key string, value []byte) bool {
	kptr := inlineptr.String(key)
	if kptr.Pointer() {
		kptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, key...)
	}

	vptr := inlineptr.Bytes(value)
	if vptr.Pointer() {
		vptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, value...)
	}

	m.set(key, entry.New(kptr, vptr))

	return m.Len() < m.Cap()
}

func (m *T) set(key string, ent entry.T) {
	prefix := ent.Key().Prefix()
	prevs := [skipMemLevels]chunkCursor{}
	cur := m.chunks[skipMemLevels-1].cursor()

	for i := skipMemLevels - 1; i >= 0; i-- {
		for cur.chunk.len > 0 {
			nprefix := cur.getPrefix()
			if nprefix > prefix {
				break
			}

			if prefix == nprefix {
				nkptr := m.ents[cur.getIdx()].Key()

				var nkey []byte
				if nkptr.Pointer() {
					begin := nkptr.Offset()
					end := begin + uint64(nkptr.Length())
					nkey = m.data[begin:end]
				} else {
					nkey = nkptr.InlineData()
				}
				if string(nkey) >= key {
					break
				}
			}

			before := cur.chunk
			if !cur.right() {
				break
			}

			// check if we need to fix up our down pointer
			if before != cur.chunk && i < skipMemLevels-1 && cur.chunk.len > 0 &&
				cur.chunk.data[0].prefix < prevs[i+1].chunk.data[0].prefix {
				prevs[i+1].chunk.down = cur.chunk
			}
		}

		prevs[i] = cur
		cur.down()
	}

	if len(m.ents) > 0 {
		ncent := prevs[0].get()
		if prefix == ncent.prefix {
			nkptr := m.ents[ncent.idx].Key()

			var nkey []byte
			if nkptr.Pointer() {
				begin := nkptr.Offset()
				end := begin + uint64(nkptr.Length())
				nkey = m.data[begin:end]
			} else {
				nkey = nkptr.InlineData()
			}

			if string(nkey) == key {
				m.ents[ncent.idx] = ent
				return
			}
		}
	}

	cent := chunkEntry{
		prefix: prefix,
		idx:    uint32(len(m.ents)),
	}
	m.ents = append(m.ents, ent)

	level := bits.TrailingZeros32(m.rng.Uint32()|(skipMemSize/2)) / skipMemRatio
	atomic.AddUint64(&Buckets[level], 1)

	for i := 0; i <= level; i++ {
		prevs[i].insert(cent)
	}
}

var Buckets [skipMemLevels]uint64
