package wal

import (
	"io"
	"unsafe"

	"github.com/zeebo/mon/internal/lsm/buffer"
	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/file"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
)

type T struct {
	fh   file.T
	cap  int
	buf  []byte
	sync bool
}

func New(fh file.T, sync bool) *T {
	var w T
	w.Init(fh, sync)
	return &w
}

func (w *T) Init(fh file.T, sync bool) {
	w.fh = fh
	w.cap = buffer.Size
	w.buf = make([]byte, 0, buffer.Size)
	w.sync = sync
}

func (w *T) Close() error {
	return w.fh.Close()
}

func (w *T) Truncate() error {
	if _, err := w.fh.Seek(0, io.SeekStart); err != nil {
		return err
	} else if err := w.fh.Truncate(0); err != nil {
		return err
	}
	return nil
}

func (w *T) Flush() error {
	size := w.cap
	if size > len(w.buf) {
		size = len(w.buf)
	}

	_, err := w.fh.Write(w.buf[:size])
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

func (w *T) AddBytes(key, value []byte) error {
	return w.AddString(*(*string)(unsafe.Pointer(&key)), value)
}

func (w *T) AddString(key string, value []byte) error {
	ent := entry.New(inlineptr.String(key), inlineptr.Bytes(value))

	w.buf = append(w.buf, ent[:]...)
	if ent.Key().Pointer() {
		w.buf = append(w.buf, key...)
	}
	if ent.Value().Pointer() {
		w.buf = append(w.buf, value...)
	}
	if len(w.buf) >= buffer.Size || w.sync {
		return w.Flush()
	}
	return nil
}
