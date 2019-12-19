package skipmem

import (
	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
)

type Iterator struct {
	m   *T
	cur chunkCursor
	end bool

	ent entry.T
	key []byte
	val []byte
}

func (i *Iterator) Next() (ok bool) {
	if i.end {
		return false
	}
	cent := i.cur.get()
	i.end = !i.cur.right()

	i.ent = i.m.ents[cent.idx]
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
