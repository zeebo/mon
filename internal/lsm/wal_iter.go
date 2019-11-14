package lsm

import (
	"io"
)

type walIterator struct {
	buf buffer
}

func newWALIterator(r io.Reader) walIterator {
	return walIterator{buf: newBufferSize(r, 4096)}
}

func (w *walIterator) Consumed() (int64, bool) { return w.buf.Consumed() }

// Next returns the next entry, the key it's for, and an error. It returns
// io.EOF when there are no more entries and the reader has no more bytes.
func (w *walIterator) Next() (ent entry, key, value []byte, err error) {
	read := 0

	data, ok := w.buf.Read(read + int(entrySize))
	if !ok {
		goto bad
	}
	copy(ent[:], data)
	read += int(entrySize)

	if ent.Key().Pointer() {
		key, ok = w.buf.Read(read + ent.Key().Length())
		if !ok {
			goto bad
		}
		key = key[read:]
		read += ent.Key().Length()
	} else if ent.Key().Inline() {
		key = ent.Key().InlineData()
	}

	if ent.Value().Pointer() {
		value, ok = w.buf.Read(read + ent.Value().Length())
		if !ok {
			goto bad
		}
		value = value[read:]
		read += ent.Value().Length()
	} else if ent.Value().Inline() {
		value = ent.Value().InlineData()
	}

	if !w.buf.Consume(read) {
		goto bad
	}

	return ent, key, value, nil

bad:
	return entry{}, nil, nil, w.buf.Error()
}
