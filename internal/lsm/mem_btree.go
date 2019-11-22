package lsm

type btreeMem struct {
	bt    btree
	cap   uint64
	data  []byte
	vptrs []inlinePtr
}

func newBtreeMem(cap uint64) *btreeMem {
	var m btreeMem
	initBtreeMem(&m, cap)
	return &m
}

func (*btreeMem) newMem(cap uint64) *btreeMem {
	return newBtreeMem(cap)
}

func initBtreeMem(m *btreeMem, cap uint64) {
	m.cap = cap
	m.data = make([]byte, 0, cap)
	m.vptrs = make([]inlinePtr, 0)
}

func (m *btreeMem) iter() btreeIterator {
	return m.bt.Iterator(m.data, m.vptrs)
}

func (m *btreeMem) reset() {
	m.bt.Reset()
	m.data = m.data[:0]
	m.vptrs = m.vptrs[:0]
}

func (m *btreeMem) Len() uint64 { return entrySize*uint64(m.bt.count) + uint64(len(m.data)) }
func (m *btreeMem) Cap() uint64 { return m.cap }

func (m *btreeMem) SetString(key string, value []byte) bool {
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

	m.bt.Insert(kptr, uint32(len(m.vptrs)), []byte(key), m.data)
	m.vptrs = append(m.vptrs, vptr)

	return m.Len() < m.Cap()
}

func (m *btreeMem) SetBytes(key, value []byte) bool {
	vptr := newInlinePtrBytes(value)
	if vptr.Pointer() {
		vptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, value...)
	}

	kptr := newInlinePtrBytes(key)
	if kptr.Pointer() {
		kptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, key...)
	}

	m.bt.Insert(kptr, uint32(len(m.vptrs)), key, m.data)
	m.vptrs = append(m.vptrs, vptr)

	return entrySize*uint64(m.bt.count)+uint64(len(m.data)) < m.cap
}
