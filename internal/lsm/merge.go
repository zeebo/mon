package lsm

import (
	"io"

	"github.com/zeebo/errs"
)

type mergeIter interface {
	inlinePtrReader

	Next() (entry, error)
}

type merger struct {
	iters []mergeIter
	eh    entryHeap
	prev  struct {
		key []byte
		ptr inlinePtr
	}
}

func (m *merger) prevKey() []byte {
	if m.prev.key != nil {
		return m.prev.key
	}
	return m.prev.ptr.InlineData()
}

func newMerger(iters []mergeIter) (*merger, error) {
	var mi merger
	err := initMerger(&mi, iters)
	return &mi, err
}

func initMerger(m *merger, iters []mergeIter) error {
	eles := make([]entryHeapElement, 0, len(iters))
	for idx, iter := range iters {
		var ele entryHeapElement
		ok, err := m.readElement(iter, &ele)
		if err != nil {
			return err
		} else if ok {
			ele.idx = idx
			eles = append(eles, ele)
		}
	}

	initEntryHeap(&m.eh, eles)
	m.iters = iters
	return nil
}

func (m *merger) readElement(iter mergeIter, ele *entryHeapElement) (ok bool, err error) {
	ele.ent, err = iter.Next()
	if err == io.EOF {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if kptr := ele.ent.Key(); kptr.Pointer() {
		ele.mkey, err = iter.AppendPointer(*kptr, nil) // sadness
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (m *merger) Next() (ele entryHeapElement, r inlinePtrReader, err error) {
again:
	ele, ok := m.eh.Pop()
	if !ok {
		return entryHeapElement{}, nil, io.EOF
	}

	if ele.idx < 0 || ele.idx >= len(m.iters) {
		return entryHeapElement{}, nil, errs.New("invalid iterator state")
	}

	iter := m.iters[ele.idx]
	var nele entryHeapElement
	ok, err = m.readElement(iter, &nele)
	if err != nil {
		return entryHeapElement{}, nil, err
	} else if ok {
		nele.idx = ele.idx
		m.eh.Push(nele)
	}

	if !m.prev.ptr.Null() && m.prev.ptr.Prefix() == ele.ent.Key().Prefix() {
		key := ele.mkey
		if key == nil {
			key = ele.ent.Key().InlineData()
		}
		if string(key) == string(m.prevKey()) {
			goto again
		}
	}

	m.prev.key = ele.mkey
	m.prev.ptr = *ele.ent.Key()
	return ele, iter, nil
}
