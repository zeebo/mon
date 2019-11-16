package lsm

import (
	"io"
	"os"
)

type wal struct {
	fh   *os.File
	sync bool
}

func newWAL(fh *os.File, sync bool) *wal {
	var w wal
	initWal(&w, fh, sync)
	return &w
}

func initWal(w *wal, fh *os.File, sync bool) {
	w.fh = fh
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

func (w *wal) AddString(key string, value []byte) error {
	ent := newEntry(newInlinePtrString(key), newInlinePtrBytes(value))

	if _, err := w.fh.Write(ent[:]); err != nil {
		return err
	}

	if ent.Key().Pointer() {
		if _, err := w.fh.WriteString(key); err != nil {
			return err
		}
	}

	if ent.Value().Pointer() {
		if _, err := w.fh.Write(value); err != nil {
			return err
		}
	}

	if w.sync {
		if err := w.fh.Sync(); err != nil {
			return err
		}
	}

	return nil
}
