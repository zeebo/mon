package btreemem

import (
	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
)

// Iterator walks over the entries in a btree.
type Iterator struct {
	b     *btree
	n     *btreeNode
	i     uint16
	buf   []byte
	vptrs []inlineptr.T

	ent entry.T
	key []byte
	val []byte
}

func (i *Iterator) Next() bool {
	if i.n == nil {
		return false
	}
	i.i++

next:
	if i.i < i.n.count {
		i.cache()
		return true
	}

	if i.n.next == invalidNode {
		i.n = nil
		return false
	}

	i.n = i.b.nodes[i.n.next]
	i.i = 0
	goto next
}

func (i *Iterator) cache() {
	bent := i.n.payload[i.i]
	i.ent = entry.New(bent.kptr, i.vptrs[bent.val])

	switch kptr := i.ent.Key(); kptr[0] {
	case inlineptr.Inline:
		i.key = append(i.key[:0], kptr.InlineData()...)
	case inlineptr.Pointer:
		begin := kptr.Offset()
		end := begin + uint64(kptr.Length())
		i.key = i.buf[begin:end]
	}

	switch vptr := i.ent.Value(); vptr[0] {
	case inlineptr.Inline:
		i.val = append(i.val[:0], vptr.InlineData()...)
	case inlineptr.Pointer:
		begin := vptr.Offset()
		end := begin + uint64(vptr.Length())
		i.val = i.buf[begin:end]
	}
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
