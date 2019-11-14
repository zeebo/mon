package lsm

import (
	"os"

	"github.com/zeebo/mon/internal/inlbytes"
)

type wal struct {
	len uint64
	cap uint64
	fh  *os.File
}

func newWAL(fh *os.File, cap uint64) *wal {
	var w wal
	initWal(&w, fh, cap)
	return &w
}

func initWal(w *wal, fh *os.File, cap uint64) {
	w.fh = fh
	w.cap = cap
}

func (w *wal) Len() uint64 { return w.len }
func (w *wal) Cap() uint64 { return w.cap }

func (w *wal) AddString(key string, value []byte) (bool, error) {
	return w.writeInline(inlbytes.FromString(key), inlbytes.FromBytes(value))
}

func (w *wal) AddBytes(key []byte, value []byte) (bool, error) {
	return w.writeInline(inlbytes.FromBytes(key), inlbytes.FromBytes(value))
}

func (w *wal) DelString(key string) (bool, error) {
	return w.writeInline(inlbytes.FromString(key), inlbytes.T{})
}

func (w *wal) DelBytes(key []byte) (bool, error) {
	return w.writeInline(inlbytes.FromBytes(key), inlbytes.T{})
}

func (w *wal) writeInline(key, value inlbytes.T) (bool, error) {
	ent := newEntry(newInlinePtrBytes(key.Bytes()), newInlinePtrBytes(value.Bytes()))

	if _, err := w.fh.Write(ent[:]); err != nil {
		return false, err
	}
	w.len += entrySize

	if ent.Key().Pointer() {
		if _, err := w.fh.Write(key.Bytes()); err != nil {
			return false, err
		}
		w.len += uint64(ent.Key().Length())
	}

	if ent.Value().Pointer() {
		if _, err := w.fh.Write(value.Bytes()); err != nil {
			return false, err
		}
		w.len += uint64(ent.Value().Length())
	}

	// stupid benchmarks
	if w.fh != nullFile {
		if err := w.fh.Sync(); err != nil {
			return false, err
		}
	}

	return w.len < w.cap, nil
}
