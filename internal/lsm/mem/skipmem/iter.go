package skipmem

import (
	"sync/atomic"

	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
)

type Iterator struct {
	m  *T
	id uint32

	ent entry.T
	key []byte
	val []byte
}

func (i *Iterator) Next() bool {
	i.id = atomic.LoadUint32(&i.m.ptrs[i.id].ptrs[0])
	if i.id == 0 {
		return false
	}

	sent := &i.m.ents[i.id]
	i.ent = entry.New(sent.kptr, i.m.vptrs[sent.val])

	switch kptr := i.ent.Key(); kptr[0] {
	case inlineptr.Inline:
		i.key = append(i.key[:0], kptr.InlineData()...)
	case inlineptr.Pointer:
		begin := kptr.Offset()
		end := begin + uint64(kptr.Length())
		i.key = i.m.data[begin:end]
	}

	switch vptr := i.ent.Value(); vptr[0] {
	case inlineptr.Inline:
		i.val = append(i.val[:0], vptr.InlineData()...)
	case inlineptr.Pointer:
		begin := vptr.Offset()
		end := begin + uint64(vptr.Length())
		i.val = i.m.data[begin:end]
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
