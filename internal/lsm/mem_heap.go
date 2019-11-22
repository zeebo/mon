package lsm

import (
	"unsafe"
)

type heapMem struct {
	cap  uint64
	heap []entry
	keys map[string]*entry
	data []byte

	ent entry
	key []byte
	val []byte
}

func newHeapMem(cap uint64) *heapMem {
	var m heapMem
	initHeapMem(&m, cap)
	return &m
}

func (*heapMem) newMem(cap uint64) *heapMem {
	return newHeapMem(cap)
}

func initHeapMem(m *heapMem, cap uint64) {
	m.cap = cap
	m.keys = make(map[string]*entry)
	m.data = make([]byte, 0, cap)
}

func (m *heapMem) iter() heapMem {
	return heapMem{
		cap:  m.cap,
		heap: append([]entry(nil), m.heap...),
		keys: m.keys,
		data: m.data,
	}
}

func (m *heapMem) reset() {
	m.data = m.data[:0]
	m.heap = m.heap[:0]
	for key := range m.keys {
		delete(m.keys, key)
	}
}

func (m *heapMem) Len() uint64 { return entrySize*uint64(len(m.keys)) + uint64(len(m.data)) }
func (m *heapMem) Cap() uint64 { return m.cap }

func (m *heapMem) SetBytes(key, value []byte) bool {
	return m.SetString(*(*string)(unsafe.Pointer(&key)), value)
}

func (m *heapMem) SetString(key string, value []byte) bool {
	vptr := newInlinePtrBytes(value)
	if vptr.Pointer() {
		vptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, value...)
	}

	eptr, ok := m.keys[key]
	if !ok {
		kptr := newInlinePtrString(key)
		if kptr.Pointer() {
			kptr.SetOffset(uint64(len(m.data)))
			m.data = append(m.data, key...)
		}
		m.heap = append(m.heap, newEntry(kptr, vptr))
		m.keys[key] = &m.heap[len(m.heap)-1]
		m.heapUp(m.heap)
	} else {
		*eptr.Value() = vptr
	}

	return m.Len() < m.Cap()
}

func (m *heapMem) readInlinePtr(ptr *inlinePtr) []byte {
	if ptr.Pointer() {
		begin := ptr.Offset()
		end := begin + uint64(ptr.Length())
		if begin < end && begin < uint64(len(m.data)) && end < uint64(len(m.data)) {
			return m.data[begin:end]
		}
	}
	return nil
}

func (m *heapMem) inlinePtrLess(i, j *inlinePtr) bool {
	if ip, jp := i.Prefix(), j.Prefix(); ip < jp {
		return true
	} else if ip > jp {
		return false
	} else {
		ki := m.readInlinePtr(i)
		if ki == nil {
			ki = i.InlineData()
		}
		kj := m.readInlinePtr(j)
		if kj == nil {
			kj = j.InlineData()
		}

		return string(ki) < string(kj)
	}
}

func (m *heapMem) heapUp(ptrs []entry) {
	i := len(ptrs) - 1
	if i < 0 || i >= len(ptrs) {
		return
	}
	ptri := ptrs[i].Key()
	ip := ptri.Prefix()

next:
	j := (i - 1) / 2
	if i != j && j >= 0 && j < len(ptrs) {
		ptrj := ptrs[j].Key()
		jp := ptrj.Prefix()

		if ip > jp {
			return
		} else if ip == jp {
			ki := m.readInlinePtr(ptri)
			if ki == nil {
				ki = ptri.InlineData()
			}
			kj := m.readInlinePtr(ptrj)
			if kj == nil {
				kj = ptrj.InlineData()
			}
			if string(ki) >= string(kj) {
				return
			}
		}

		*ptri, *ptrj = *ptrj, *ptri
		ptri, i, ip = ptrj, j, jp
		goto next
	}
}

func (m *heapMem) heapDown(ptrs []entry) {
	if len(ptrs) == 0 {
		return
	}
	ptri, i := ptrs[0].Key(), 0
	ip := ptri.Prefix()

next:
	j1 := 2*i + 1
	if j1 >= 0 && j1 < len(ptrs) {
		ptrj, j := ptrs[j1].Key(), j1
		jp := ptrj.Prefix()

		if j2 := j1 + 1; j2 >= 0 && j2 < len(ptrs) {
			ptrj2 := ptrs[j2].Key()
			jp2 := ptrj2.Prefix()

			if jp2 < jp {
				ptrj, j, jp = ptrj2, j2, jp2
			} else if jp2 == jp {
				kj := m.readInlinePtr(ptrj)
				if kj == nil {
					kj = ptrj.InlineData()
				}
				kj2 := m.readInlinePtr(ptrj2)
				if kj2 == nil {
					kj2 = ptrj2.InlineData()
				}
				if string(kj2) < string(kj) {
					ptrj, j, jp = ptrj2, j2, jp2
				}
			}
		}

		if ip > jp {
			return
		} else if ip == jp {
			ki := m.readInlinePtr(ptri)
			if ki == nil {
				ki = ptri.InlineData()
			}
			kj := m.readInlinePtr(ptrj)
			if kj == nil {
				kj = ptrj.InlineData()
			}
			if string(ki) >= string(kj) {
				return
			}
		}

		*ptri, *ptrj = *ptrj, *ptri
		ptri, i, ip = ptrj, j, jp
		goto next
	}
}

func (m *heapMem) Next() bool {
	heap := m.heap

	if len(heap) == 0 {
		return false
	}

	n := len(heap) - 1
	m.ent = heap[0]
	heap[0] = heap[n]
	m.heapDown(heap)
	m.heap = heap[:n]

	switch kptr := m.ent.Key(); kptr[0] {
	case inlinePtr_Inline:
		m.key = append(m.key[:0], kptr.InlineData()...)
	case inlinePtr_Pointer:
		begin := kptr.Offset()
		end := begin + uint64(kptr.Length())
		m.key = m.data[begin:end]
	case inlinePtr_Null:
		m.key = nil
	}

	switch vptr := m.ent.Value(); vptr[0] {
	case inlinePtr_Inline:
		m.val = append(m.val[:0], vptr.InlineData()...)
	case inlinePtr_Pointer:
		begin := vptr.Offset()
		end := begin + uint64(vptr.Length())
		m.val = m.data[begin:end]
	case inlinePtr_Null:
		m.val = nil
	}

	return true
}

func (m *heapMem) Entry() entry  { return m.ent }
func (m *heapMem) Key() []byte   { return m.key }
func (m *heapMem) Value() []byte { return m.val }

func (m *heapMem) Err() error { return nil }
