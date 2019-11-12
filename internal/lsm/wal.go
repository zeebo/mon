package lsm

import (
	"io"

	"github.com/zeebo/mon/internal/inlbytes"
)

type WriteFlusher interface {
	io.Writer
	Flush() error
}

type WAL struct {
	len uint64
	cap uint64
	wf  WriteFlusher
}

func NewWAL(wf WriteFlusher, cap uint64) *WAL {
	var w WAL
	return newWal(&w, wf, cap)
}

func NewWALUnsafe(w io.Writer, cap uint64) *WAL {
	return NewWAL(noopFlusher{w}, cap)
}

func newWal(w *WAL, wf WriteFlusher, cap uint64) *WAL {
	w.wf = wf
	w.cap = cap
	return w
}

func (w *WAL) Len() uint64 { return w.len }
func (w *WAL) Cap() uint64 { return w.cap }

func (w *WAL) AddString(key string, value []byte) (bool, error) {
	return w.writeInline(inlbytes.FromString(key), inlbytes.FromBytes(value))
}

func (w *WAL) AddBytes(key []byte, value []byte) (bool, error) {
	return w.writeInline(inlbytes.FromBytes(key), inlbytes.FromBytes(value))
}

func (w *WAL) DelString(key string) (bool, error) {
	return w.writeInline(inlbytes.FromString(key), inlbytes.T{})
}

func (w *WAL) DelBytes(key []byte) (bool, error) {
	return w.writeInline(inlbytes.FromBytes(key), inlbytes.T{})
}

func (w *WAL) writeInline(key, value inlbytes.T) (bool, error) {
	ent := newEntry(newInlinePtrBytes(key.Bytes()), newInlinePtrBytes(value.Bytes()))

	data := make([]byte, 0, len(ent)+ent.Key().Length()+ent.Value().Length())
	data = append(data, ent[:]...)
	data = append(data, key.Bytes()...)
	data = append(data, value.Bytes()...)

	if _, err := w.wf.Write(data); err != nil {
		return false, err
	} else if err := w.wf.Flush(); err != nil {
		return false, err
	} else {
		w.len += entrySize + uint64(ent.Key().Length()) + uint64(ent.Value().Length())
		return w.len < w.cap, nil
	}
}
