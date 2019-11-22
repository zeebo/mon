package lsm

type skipMemChunk struct {
}

type skipMem struct {
	bt    btree
	cap   uint64
	data  []byte
	vptrs []inlinePtr
}

func newSkipMem(cap uint64) *skipMem {
	var m skipMem
	initSkipMem(&m, cap)
	return &m
}

func (*skipMem) newMem(cap uint64) *skipMem {
	return newSkipMem(cap)
}

func initSkipMem(m *skipMem, cap uint64) {
	m.cap = cap
	m.data = make([]byte, 0, cap)
	m.vptrs = make([]inlinePtr, 0)
}

func (m *skipMem) iter() btreeIterator {
	return m.bt.Iterator(m.data, m.vptrs)
}

func (m *skipMem) reset() {
	m.bt.Reset()
	m.data = m.data[:0]
	m.vptrs = m.vptrs[:0]
}

func (m *skipMem) Len() uint64 { return entrySize*uint64(m.bt.count) + uint64(len(m.data)) }
func (m *skipMem) Cap() uint64 { return m.cap }

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

	m.bt.Insert(kptr, uint32(len(m.vptrs)), []byte(key), m.data)
	m.vptrs = append(m.vptrs, vptr)

	return m.Len() < m.Cap()
}

func (m *skipMem) SetBytes(key, value []byte) bool {
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
