package heapmem

import (
	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
)

type Iterator struct {
	cap  uint64
	heap []entry.T
	keys map[string]*entry.T
	data []byte

	ent entry.T
	key []byte
	val []byte
}

func (i *Iterator) Next() bool {
	heap, data := i.heap, i.data

	if len(heap) == 0 {
		return false
	}

	n := len(heap) - 1
	i.ent = heap[0]
	heap[0] = heap[n]
	heapDown(data, heap)
	i.heap = heap[:n]

	switch kptr := i.ent.Key(); kptr[0] {
	case inlineptr.Inline:
		i.key = append(i.key[:0], kptr.InlineData()...)
	case inlineptr.Pointer:
		begin := kptr.Offset()
		end := begin + uint64(kptr.Length())
		i.key = i.data[begin:end]
	}

	switch vptr := i.ent.Value(); vptr[0] {
	case inlineptr.Inline:
		i.val = append(i.val[:0], vptr.InlineData()...)
	case inlineptr.Pointer:
		begin := vptr.Offset()
		end := begin + uint64(vptr.Length())
		i.val = i.data[begin:end]
	}

	return true
}

func (i *Iterator) Entry() entry.T { return i.ent }

func (i *Iterator) Key() []byte {
	if i.ent.Key().Null() {
		return nil
	}
	return i.key
}

func (i *Iterator) Value() []byte {
	if i.ent.Value().Null() {
		return nil
	}
	return i.val
}

func (i *Iterator) Err() error { return nil }
