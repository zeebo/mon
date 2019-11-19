package lsm

import (
	"io"
	"os"
)

const walBufSize = 4096

type wal struct {
	fh   *os.File
	buf  []byte
	sync bool
}

func newWAL(fh *os.File, sync bool) *wal {
	var w wal
	initWal(&w, fh, sync)
	return &w
}

func initWal(w *wal, fh *os.File, sync bool) {
	w.fh = fh
	w.buf = make([]byte, 0, walBufSize)
	w.sync = sync
}

func (w *wal) Truncate() error {
	if _, err := w.fh.Seek(0, io.SeekStart); err != nil {
		return err
	} else if err := w.fh.Truncate(0); err != nil {
		return err
	}
	return nil
}

func (w *wal) Flush() error {
	_, err := w.fh.Write(w.buf)
	w.buf = w.buf[:0]

	if err != nil {
		return err
	}

	if w.sync {
		if err := w.fh.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (w *wal) AddString(key string, value []byte) error {
	ent := newEntry(newInlinePtrString(key), newInlinePtrBytes(value))

	w.buf = append(w.buf, ent[:]...)
	if ent.Key().Pointer() {
		w.buf = append(w.buf, key...)
	}
	if ent.Value().Pointer() {
		w.buf = append(w.buf, value...)
	}
	if len(w.buf) >= walBufSize || w.sync {
		return w.Flush()
	}
	return nil
}

func (w *wal) AddBytes(key, value []byte) error {
	ent := newEntry(newInlinePtrBytes(key), newInlinePtrBytes(value))

	w.buf = append(w.buf, ent[:]...)
	if ent.Key().Pointer() {
		w.buf = append(w.buf, key...)
	}
	if ent.Value().Pointer() {
		w.buf = append(w.buf, value...)
	}
	if len(w.buf) >= walBufSize || w.sync {
		return w.Flush()
	}
	return nil
}
