package layermem

import (
	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
)

type layerEntriesIterator struct {
	data  []byte
	ents  []entry.T
	lents []layerEntry

	ok  bool
	ent entry.T
	key []byte
	val []byte
}

func (i *layerEntriesIterator) Next() bool {
	lents, ents, data := i.lents, i.ents, i.data

	if len(lents) == 0 {
		return false
	}

	lent := lents[0]
	i.lents = lents[1:]
	i.ok = false

	i.ent = ents[lent.entry]

	switch kptr := i.ent.Key(); kptr[0] {
	case inlineptr.Inline:
		i.key = append(i.key[:0], kptr.InlineData()...)
	case inlineptr.Pointer:
		begin := kptr.Offset()
		end := begin + uint64(kptr.Length())
		i.key = data[begin:end]
	}

	switch vptr := i.ent.Value(); vptr[0] {
	case inlineptr.Inline:
		i.val = append(i.val[:0], vptr.InlineData()...)
	case inlineptr.Pointer:
		begin := vptr.Offset()
		end := begin + uint64(vptr.Length())
		i.val = data[begin:end]
	}

	return true
}

func (i *layerEntriesIterator) Entry() entry.T {
	return i.ent
}

func (i *layerEntriesIterator) Key() []byte {
	if i.ent.Key().Null() {
		return nil
	}
	return i.key
}

func (i *layerEntriesIterator) Value() []byte {
	if i.ent.Value().Null() {
		return nil
	}
	return i.val
}

func (i *layerEntriesIterator) Err() error { return nil }
