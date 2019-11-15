package lsm

import (
	"io"

	"github.com/zeebo/errs"
)

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

func (m *mem) iterClone() *mem {
	return &mem{
		cap:  m.cap,
		heap: append([]inlinePtr(nil), m.heap...),
		keys: m.keys,
		data: m.data,
	}
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
		begin := ptr.Offset()
		end := begin + uint64(ptr.Length())
		if begin < end && begin < uint64(len(m.data)) && end < uint64(len(m.data)) {
			return m.data[begin:end]
		}
	}
	return nil
}

func (m *mem) inlinePtrLess(i, j *inlinePtr) bool {
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

func (m *mem) Next() (entry, error) {
	heap := m.heap

	if len(heap) == 0 {
		return entry{}, io.EOF
	}

	n := len(heap) - 1
	kptr := heap[0]
	heap[0] = heap[n]
	m.heapDown(heap)
	m.heap = heap[:n]

	var key []byte
	if kptr.Pointer() {
		var err error
		key, err = m.ReadPointer(kptr)
		if err != nil {
			return entry{}, err
		}
	} else if kptr.Inline() {
		key = kptr.InlineData()
	}

	kdata, ok := m.keys[string(key)]
	if !ok {
		return entry{}, errs.New("invalid memory heap")
	}

	return newEntry(kptr, kdata.vptr), nil
}

func (m *mem) ReadPointer(ptr inlinePtr) ([]byte, error) {
	begin := ptr.Offset()
	end := begin + uint64(ptr.Length())
	if begin <= end && begin < uint64(len(m.data)) && end <= uint64(len(m.data)) {
		return m.data[begin:end], nil
	}
	return nil, errs.New("invalid pointer read: %d[%d:%d]", len(m.data), begin, end)
}
