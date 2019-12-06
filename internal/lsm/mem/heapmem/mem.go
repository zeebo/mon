package heapmem

import (
	"unsafe"

	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
	"github.com/zeebo/mon/internal/lsm/iterator"
)

type T struct {
	cap  uint64
	heap []entry.T
	keys map[string]*entry.T
	data []byte
}

func (m *T) Init(cap uint64) {
	m.cap = cap
	m.keys = make(map[string]*entry.T)
	m.data = make([]byte, 0, cap)
	m.heap = make([]entry.T, 0, cap/entry.Size)
}

func (m *T) Iter() Iterator {
	return Iterator{
		cap:  m.cap,
		heap: append([]entry.T(nil), m.heap...),
		keys: m.keys,
		data: m.data,
	}
}

func (m *T) Iters() []iterator.T {
	it := m.Iter()
	return []iterator.T{&it}
}

func (m *T) Reset() {
	m.data = m.data[:0]
	m.heap = m.heap[:0]
	for key := range m.keys {
		delete(m.keys, key)
	}
}

func (m *T) Keys() uint32 { return uint32(len(m.keys)) }
func (m *T) Cap() uint64  { return m.cap }
func (m *T) Len() uint64  { return entry.Size*uint64(len(m.keys)) + uint64(len(m.data)) }

func (m *T) SetBytes(key, value []byte) bool {
	return m.SetString(*(*string)(unsafe.Pointer(&key)), value)
}

func (m *T) SetString(key string, value []byte) bool {
	vptr := inlineptr.Bytes(value)
	if vptr.Pointer() {
		vptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, value...)
	}

	eptr, ok := m.keys[key]
	if !ok {
		kptr := inlineptr.String(key)
		if kptr.Pointer() {
			kptr.SetOffset(uint64(len(m.data)))
			m.data = append(m.data, key...)
		}
		m.heap = append(m.heap, entry.New(kptr, vptr))
		m.keys[key] = &m.heap[len(m.heap)-1]
		heapUp(m.data, m.heap)
	} else {
		*eptr.Value() = vptr
	}

	return m.Len() < m.Cap()
}
