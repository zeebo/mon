package layermem

import (
	"math/bits"
	"unsafe"

	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
	"github.com/zeebo/mon/internal/lsm/iterator"
	"github.com/zeebo/mon/internal/lsm/iterator/mergeiter"
)

const fanout = 4

type T struct {
	cap uint64

	buf  [fanout]layerEntry
	bufn int

	layers []*layer
	data   []byte
	ents   []entry.T
}

func (m *T) Init(cap uint64) {
	m.cap = cap
	m.data = make([]byte, 0, cap)
	m.ents = make([]entry.T, 0, cap/entry.Size)
}

func (m *T) Reset() {
	m.data = m.data[:0]
	m.ents = m.ents[:0]
	m.bufn = 0
	for _, layer := range m.layers {
		layer.reset()
	}
}

func (m *T) Keys() uint32 { return uint32(len(m.ents)) }
func (m *T) Cap() uint64  { return m.cap }
func (m *T) Len() uint64  { return entry.Size*uint64(len(m.ents)) + uint64(len(m.data)) }

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

	m.buf[m.bufn%fanout] = layerEntry{
		prefix: kptr.Prefix(),
		entry:  uint32(len(m.ents)),
	}
	m.bufn++

	m.ents = append(m.ents, entry.New(kptr, vptr))

	if m.bufn == fanout {
		m.bufn = 0
		m.flush()
	}

	return m.Len() < m.Cap()
}

func (m *T) Iters() (is []iterator.T) {
	if m.bufn > 0 && m.bufn <= fanout {
		is = append(is, &layerEntriesIterator{
			data:  m.data,
			ents:  m.ents,
			lents: append([]layerEntry(nil), m.buf[:m.bufn]...),
		})

	}

	for _, layer := range m.layers {
		for i := 0; i < layer.n && i < fanout; i++ {
			is = append(is, &layerEntriesIterator{
				data:  m.data,
				ents:  m.ents,
				lents: layer.data[i],
			})
		}
	}

	return []iterator.T{mergeiter.New(is)}
}

func (m *T) flush() {
	current := m.flushUp(0)
	into := current.data[current.n%fanout][:0]
	heads := m.buf

	for i := 0; i < fanout; i++ {
		first, second := min2Entries4(&heads)
		first %= fanout

		ent := heads[first]
		pre := ent.prefix

		if bits.TrailingZeros64(pre) < 8 && pre == heads[second%fanout].prefix {
			first = findSmallest(m, first, heads) % fanout
			ent = heads[first]
		}

		into = append(into, ent)
		heads[first].prefix = largestPrefix
	}

	current.data[current.n%fanout] = into
	current.n++
}

func (m *T) flushUp(idx int) *layer {
	if idx >= len(m.layers) {
		current := new(layer)
		m.layers = append(m.layers, current)
		return current
	}

	current := m.layers[idx]
	if !current.full() {
		return current
	}

	parent := m.flushUp(idx + 1)
	parent.data[parent.n%fanout] = current.merge(m, parent.data[parent.n%fanout][:0])
	parent.bump()

	// TODO: reuse current if there's no iterators holding it
	current.reset()
	// current = new(layer)
	// m.layers[idx] = current

	return current
}
