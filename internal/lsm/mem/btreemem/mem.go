package btreemem

import (
	"unsafe"

	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
	"github.com/zeebo/mon/internal/lsm/iterator"
)

type T struct {
	bt    btree
	cap   uint64
	data  []byte
	vptrs []inlineptr.T
}

func (m *T) Init(cap uint64) {
	m.cap = cap
	m.data = make([]byte, 0, cap)
	m.vptrs = make([]inlineptr.T, 0, cap/entry.Size)
}

func (m *T) Iter() Iterator {
	return m.bt.Iterator(m.data, m.vptrs)
}

func (m *T) Iters() []iterator.T {
	it := m.Iter()
	return []iterator.T{&it}
}

func (m *T) Reset() {
	m.bt.Reset()
	m.data = m.data[:0]
	m.vptrs = m.vptrs[:0]
}

func (m *T) Keys() uint32 { return m.bt.count }
func (m *T) Cap() uint64  { return m.cap }
func (m *T) Len() uint64  { return entry.Size*uint64(m.bt.count) + uint64(len(m.data)) }

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

	m.bt.Insert(kptr, uint32(len(m.vptrs)), []byte(key), m.data)
	m.vptrs = append(m.vptrs, vptr)

	return m.Len() < m.Cap()
}
