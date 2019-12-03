package lsm

// btreeIterator walks over the entries in a btree.
type btreeIterator struct {
	b     *btree
	n     *btreeNode
	i     uint16
	buf   []byte
	vptrs []inlinePtr

	ent entry
	key []byte
	val []byte
}

// func (i *btreeIterator) AppendPointer(ptr inlinePtr, buf []byte) ([]byte, error) {
// 	begin := ptr.Offset()
// 	end := begin + uint64(ptr.Length())
// 	if begin <= end && begin <= uint64(len(i.buf)) && end <= uint64(len(i.buf)) {
// 		return append(buf, i.buf[begin:end]...), nil
// 	}
// 	return nil, errs.New("invalid pointer read: %d[%d:%d]", len(i.buf), begin, end)
// }

func (i *btreeIterator) Next() bool {
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

func (i *btreeIterator) cache() {
	bent := i.n.payload[i.i]
	i.ent = newEntry(bent.kptr, i.vptrs[bent.val])

	switch kptr := i.ent.Key(); kptr[0] {
	case inlinePtr_Inline:
		i.key = append(i.key[:0], kptr.InlineData()...)
	case inlinePtr_Pointer:
		begin := kptr.Offset()
		end := begin + uint64(kptr.Length())
		i.key = i.buf[begin:end]
	}

	switch vptr := i.ent.Value(); vptr[0] {
	case inlinePtr_Inline:
		i.val = append(i.val[:0], vptr.InlineData()...)
	case inlinePtr_Pointer:
		begin := vptr.Offset()
		end := begin + uint64(vptr.Length())
		i.val = i.buf[begin:end]
	}
}

func (i *btreeIterator) Entry() entry { return i.ent }

func (i *btreeIterator) Key() []byte {
	if i.ent.Key().Null() {
		return nil
	}
	return i.key
}

func (i *btreeIterator) Value() []byte {
	if i.ent.Value().Null() {
		return nil
	}
	return i.val
}

func (i *btreeIterator) Err() error { return nil }
