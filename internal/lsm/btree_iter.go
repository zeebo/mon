package lsm

import (
	"io"

	"github.com/zeebo/errs"
)

// btreeIterator walks over the entries in a btree.
type btreeIterator struct {
	b     *btree
	n     *btreeNode
	i     uint16
	buf   []byte
	vptrs []inlinePtr
}

func (i *btreeIterator) Next() (entry, error) {
	if !i.Advance() {
		return entry{}, io.EOF
	}
	bent := i.n.payload[i.i]
	return newEntry(bent.kptr, i.vptrs[bent.val]), nil
}

func (i *btreeIterator) AppendPointer(ptr inlinePtr, buf []byte) ([]byte, error) {
	begin := ptr.Offset()
	end := begin + uint64(ptr.Length())
	if begin <= end && begin <= uint64(len(i.buf)) && end <= uint64(len(i.buf)) {
		return append(buf, i.buf[begin:end]...), nil
	}
	return nil, errs.New("invalid pointer read: %d[%d:%d]", len(i.buf), begin, end)
}

// Advance advances the btreeIterator and returns true if there is an entry.
func (i *btreeIterator) Advance() bool {
	if i.n == nil {
		return false
	}
	i.i++

next:
	if i.i < i.n.count {
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
