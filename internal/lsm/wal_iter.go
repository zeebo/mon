package lsm

type walIterator struct {
	buf      buffer
	consumed int64
	prefix   bool
}

func newWALIterator(fh file) *walIterator {
	var wi walIterator
	initWALIterator(&wi, fh)
	return &wi
}

func initWALIterator(wi *walIterator, fh file) {
	initBuffer(&wi.buf, fh, 4096)
}

func (w *walIterator) Consumed() (int64, bool) { return w.consumed, w.prefix }

func (w *walIterator) Err() error { return w.buf.Err() }

func (w *walIterator) Next() (ent entry, key, value []byte, ok bool) {
	consumed := int64(entrySize)
	data, ok := w.buf.Read(int(entrySize))
	if !ok {
		goto bad
	}
	copy(ent[:], data)

	if ent.Key().Pointer() {
		key, ok = w.buf.Read(ent.Key().Length())
		if !ok {
			goto bad
		}
		consumed += int64(len(key))
	} else if ent.Key().Inline() {
		key = ent.Key().InlineData()
	}

	if ent.Value().Pointer() {
		value, ok = w.buf.Read(ent.Value().Length())
		if !ok {
			goto bad
		}
		consumed += int64(len(value))
	} else if ent.Value().Inline() {
		value = ent.Value().InlineData()
	}

	w.consumed += consumed
	return ent, key, value, true

bad:
	w.prefix = w.buf.Buffered() > 0
	return entry{}, nil, nil, false
}
