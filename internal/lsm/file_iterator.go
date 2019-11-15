package lsm

import (
	"reflect"
	"unsafe"

	"github.com/zeebo/errs"
)

type fileIterator struct {
	entries *handle
	values  *handle
	rem     [32]byte
	n       int
}

func newFileIterator(entries, values *handle) *fileIterator {
	var f fileIterator
	initFileIterator(&f, entries, values)
	return &f
}

func initFileIterator(f *fileIterator, entries, values *handle) {
	f.entries = entries
	f.values = values
}

func (fi *fileIterator) ReadEntries(buf []entry) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	byteBuf := *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&buf[0])),
		Len:  len(buf) * entrySize,
		Cap:  cap(buf) * entrySize,
	}))

	for {
		copy(byteBuf, fi.rem[:fi.n])
		n, err := fi.entries.fh.Read(byteBuf[fi.n:])
		n += fi.n
		fi.n = copy(fi.rem[:], byteBuf[n&^31:n])

		if n/32 > 0 || err != nil {
			return n / 32, err
		}
	}
}

func (fi *fileIterator) ReadPointer(ptr inlinePtr) ([]byte, error) {
	buf := make([]byte, ptr.Length())
	n, err := fi.values.fh.ReadAt(buf, int64(ptr.Offset()))
	if n == len(buf) {
		return buf, nil
	} else if err == nil {
		err = errs.New("short read")
	}
	return nil, err
}
