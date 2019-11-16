package lsm

import (
	"io"
	"os"
)

type wal struct {
	fh *os.File
}

func newWAL(fh *os.File) *wal {
	var w wal
	initWal(&w, fh)
	return &w
}

func initWal(w *wal, fh *os.File) {
	w.fh = fh
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

	// if err := w.fh.Sync(); err != nil {
	// 	return err
	// }

	return nil
}
