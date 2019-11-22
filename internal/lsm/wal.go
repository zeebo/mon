package lsm

import (
	"io"
	"sync/atomic"
	"time"
)

type wal struct {
	fh   file
	cap  int
	buf  []byte
	sync bool
}

func newWAL(fh file, sync bool) *wal {
	var w wal
	initWal(&w, fh, sync)
	return &w
}

func initWal(w *wal, fh file, sync bool) {
	w.fh = fh
	w.cap = bufferSize
	w.buf = make([]byte, 0, bufferSize)
	w.sync = sync
}

func (w *wal) Close() error {
	return w.fh.Close()
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
	size := w.cap
	if size > len(w.buf) {
		size = len(w.buf)
	}

	var s time.Time
	if trackStats {
		s = time.Now()
	}
	n, err := w.fh.Write(w.buf[:size])
	if trackStats {
		atomic.AddInt64(&writtenDur, time.Since(s).Nanoseconds())
		atomic.AddInt64(&written, int64(n))
	}

	w.buf = w.buf[:copy(w.buf, w.buf[size:]):cap(w.buf)]

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
	ent := newEntry(
		newInlinePtrString(key),
		newInlinePtrBytes(value))

	w.buf = append(w.buf, ent[:]...)
	if ent.Key().Pointer() {
		w.buf = append(w.buf, key...)
	}
	if ent.Value().Pointer() {
		w.buf = append(w.buf, value...)
	}
	if len(w.buf) >= bufferSize || w.sync {
		return w.Flush()
	}
	return nil
}

func (w *wal) AddBytes(key, value []byte) error {
	ent := newEntry(
		newInlinePtrBytes(key),
		newInlinePtrBytes(value))

	w.buf = append(w.buf, ent[:]...)
	if ent.Key().Pointer() {
		w.buf = append(w.buf, key...)
	}
	if ent.Value().Pointer() {
		w.buf = append(w.buf, value...)
	}
	if len(w.buf) >= bufferSize || w.sync {
		return w.Flush()
	}
	return nil
}
