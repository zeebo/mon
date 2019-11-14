package lsm

type mem struct {
	cap  uint64
	heap []inlinePtr
	keys map[string]memKeyData
	data []byte
}

type memKeyData struct {
	offset uint64
	vptr   inlinePtr
}

func newMem(cap uint64) *mem {
	var m mem
	initMem(&m, cap)
	return &m
}

func initMem(m *mem, cap uint64) {
	m.cap = cap
	m.keys = make(map[string]memKeyData)
	m.data = make([]byte, 0, cap)
}

func (m *mem) Len() uint64 { return uint64(len(m.data)) }
func (m *mem) Cap() uint64 { return m.cap }

func (m *mem) SetString(key string, value []byte) bool {
	vptr := newInlinePtrBytes(value)
	if vptr.Pointer() {
		vptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, value...)
	}

	kptr := newInlinePtrString(key)
	kdata, ok := m.keys[key]
	if !ok {
		if kptr.Pointer() {
			kdata.offset = uint64(len(m.data))
			m.data = append(m.data, key...)
		}
		m.heap = append(m.heap, kptr)
		m.heapUp(m.heap)
	}
	kdata.vptr = vptr
	m.keys[key] = kdata

	return entrySize*uint64(len(m.keys))+uint64(len(m.data)) < m.cap
}

func (m *mem) readInlinePtr(ptr *inlinePtr) []byte {
	if ptr.Pointer() {
		return m.data[ptr.Offset() : ptr.Offset()+uint64(ptr.Length())]
	}
	return ptr.InlineData()
}

func (m *mem) inlinePtrLess(i, j *inlinePtr) bool {
	if ip, jp := i.Prefix(), j.Prefix(); ip < jp {
		return true
	} else if ip > jp {
		return false
	} else {
		return string(m.readInlinePtr(i)) < string(m.readInlinePtr(j))
	}
}

func (m *mem) heapUp(ptrs []inlinePtr) {
	i := len(ptrs) - 1
	if i < 0 || i >= len(ptrs) {
		return
	}
	ptri := &ptrs[i]

next:
	j := (i - 1) / 2
	if i != j && j >= 0 && j < len(ptrs) {
		ptrj := &ptrs[j]
		if m.inlinePtrLess(ptri, ptrj) {
			*ptri, *ptrj = *ptrj, *ptri
			ptri, i = ptrj, j
			goto next
		}
	}
}

func (m *mem) heapDown(ptrs []inlinePtr) {
	if len(ptrs) == 0 {
		return
	}
	ptri, i := &ptrs[0], 0

next:
	j1 := 2*i + 1
	if j1 >= 0 && j1 < len(ptrs) {
		ptrj, j := &ptrs[j1], j1

		if j2 := j1 + 1; j2 >= 0 && j2 < len(ptrs) {
			if m.inlinePtrLess(&ptrs[j2], &ptrs[j1]) {
				ptrj, j = &ptrs[j2], j2
			}
		}

		if m.inlinePtrLess(ptrj, ptri) {
			*ptri, *ptrj = *ptrj, *ptri
			ptri, i = ptrj, j
			goto next
		}
	}
}

func (m *mem) Iterator() mergeIter {
	for len(m.heap) > 0 {
		n := len(m.heap) - 1
		kptr := m.heap[0]
		m.heap[0] = m.heap[n]
		m.heapDown(m.heap)
		m.heap = m.heap[:n]

		kdata := m.keys[string(m.readInlinePtr(&kptr))]
		_ = newEntry(kptr, kdata.vptr)
	}

	// TODO(jeff): should be an iterator
	return nil
}
