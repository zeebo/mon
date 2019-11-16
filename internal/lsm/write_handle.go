package lsm

import (
	"io"
	"os"

	"github.com/zeebo/errs"
)

type writeHandle struct {
	fh  *os.File
	off int64
	buf []byte
	cap int
}

func newWriteHandle(fh *os.File, cap int) (*writeHandle, error) {
	var h writeHandle
	return &h, initWriteHandle(&h, fh, cap)
}

func initWriteHandle(h *writeHandle, fh *os.File, cap int) (err error) {
	h.off, err = fh.Seek(0, io.SeekCurrent)
	if err != nil {
		return errs.Wrap(err)
	}

	h.fh = fh
	h.buf = make([]byte, 0, cap)
	h.cap = cap
	return nil
}

func (h *writeHandle) Offset() uint64 { return uint64(h.off) }

func (h *writeHandle) Append(p []byte) (err error) {
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

func (h *writeHandle) Flush() error {
	if len(h.buf) == 0 {
		return nil
	}

	if _, err := h.fh.Write(h.buf); err != nil {
		return err
	}

	h.buf = h.buf[:0]
	return nil
}

func (h *writeHandle) Sync() error {
	return h.fh.Sync()
}
