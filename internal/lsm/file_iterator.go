package lsm

type fileIterator struct {
	err       error
	entBuffer buffer
	valBuffer buffer
	ent       entry
	key       []byte
	val       []byte
}

func newFileIterator(entries, values file) *fileIterator {
	var fi fileIterator
	initFileIterator(&fi, entries, values)
	return &fi
}

func initFileIterator(fi *fileIterator, entries, values file) {
	initBuffer(&fi.entBuffer, entries, bufferSize)
	initBuffer(&fi.valBuffer, values, bufferSize)
}

func (fi *fileIterator) Next() bool {
	if fi.err != nil {
		return false
	}

	buf, ok := fi.entBuffer.Read(entrySize)
	if !ok {
		fi.err = fi.entBuffer.Err()
		return false
	}
	copy(fi.ent[:], buf)

	switch kptr := fi.ent.Key(); kptr[0] {
	case inlinePtr_Inline:
		fi.key = append(fi.key[:0], kptr.InlineData()...)
	case inlinePtr_Pointer:
		key, ok := fi.valBuffer.Read(kptr.Length())
		if !ok {
			fi.err = fi.valBuffer.Err()
			return false
		}
		fi.key = append(fi.key[:0], key...)
	}

	switch vptr := fi.ent.Value(); vptr[0] {
	case inlinePtr_Inline:
		fi.val = append(fi.val[:0], vptr.InlineData()...)
	case inlinePtr_Pointer:
		val, ok := fi.valBuffer.Read(vptr.Length())
		if !ok {
			fi.err = fi.valBuffer.Err()
			return false
		}
		fi.val = append(fi.val[:0], val...)
	}

	return true
}

func (fi *fileIterator) Entry() entry { return fi.ent }

func (fi *fileIterator) Key() []byte {
	if fi.ent.Key().Null() {
		return nil
	}
	return fi.key
}

func (fi *fileIterator) Value() []byte {
	if fi.ent.Value().Null() {
		return nil
	}
	return fi.val
}

func (fi *fileIterator) Err() error { return fi.err }
