package lsm

import (
	"io"
)

type WALIterator struct {
	buf buffer
}

func NewWALIterator(r io.Reader) WALIterator {
	return WALIterator{buf: newBufferSize(r, 4096)}
}

func (w *WALIterator) Consumed() (int64, bool) { return w.buf.Consumed() }

// Next returns the next entry, the key it's for, and an error. It returns
// io.EOF when there are no more entries and the reader has no more bytes.
func (w *WALIterator) Next() (ent entry, key, value []byte, err error) {
	read := 0

	data, ok := w.buf.Read(read + int(entrySize))
	if !ok {
		goto bad
	}
	copy(ent[:], data)
	read += int(entrySize)

	// a bit hacky to avoid consuming until after the key is gone
	key, ok = w.buf.Read(read + ent.Key().Length())
	if !ok {
		goto bad
	}
	key = key[read:]
	read += ent.Key().Length()

	if !ent.Value().Null() {
		value, ok = w.buf.Read(read + ent.Value().Length())
		if !ok {
			goto bad
		}
		value = value[read:]
		read += ent.Value().Length()
	}

	if !w.buf.Consume(read) {
		goto bad
	}

	return ent, key, value, nil

bad:
	return entry{}, nil, nil, w.buf.Error()
}
