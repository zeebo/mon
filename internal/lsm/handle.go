package lsm

import (
	"io"
	"os"

	"github.com/zeebo/errs"
)

type handle struct {
	fh  *os.File
	off int64
	buf []byte
	cap int
}

func newHandle(fh *os.File, cap int) (*handle, error) {
	var h handle
	return &h, initHandle(&h, fh, cap)
}

func initHandle(h *handle, fh *os.File, cap int) (err error) {
	h.off, err = fh.Seek(0, io.SeekCurrent)
	if err != nil {
		return errs.Wrap(err)
	}

	h.fh = fh
	h.buf = make([]byte, 0, cap)
	h.cap = cap
	return nil
}

func (h *handle) Offset() uint64 { return uint64(h.off) }

func (h *handle) Append(p []byte) (err error) {
	h.buf = append(h.buf, p...)
	h.off += int64(len(p))

	if len(h.buf) < h.cap {
		return nil
	}

	if _, err = h.fh.Write(h.buf); err != nil {
		return err
	}

	h.buf = h.buf[:0]
	return nil
}

func (h *handle) Flush() error {
	if len(h.buf) == 0 {
		return nil
	}

	if _, err := h.fh.Write(h.buf); err != nil {
		return err
	}

	h.buf = h.buf[:0]
	return nil
}

func (h *handle) Sync() error {
	return h.fh.Sync()
}
