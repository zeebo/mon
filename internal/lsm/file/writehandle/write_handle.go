package writehandle

import (
	"github.com/zeebo/mon/internal/lsm/file"
)

type T struct {
	fh  file.T
	off int64
	cap int
	buf []byte
}

func New(fh file.T, cap int) *T {
	var h T
	h.Init(fh, make([]byte, 0, cap))
	return &h
}

func NewBuf(fh file.T, buf []byte) *T {
	var wh T
	wh.Init(fh, buf[:0])
	return &wh
}

func (h *T) Init(fh file.T, buf []byte) {
	h.fh = fh
	h.cap = cap(buf)
	h.buf = buf
}

func (h *T) Reset() {
	h.off = 0
	h.buf = h.buf[:0]
}

func (h *T) File() file.T { return h.fh }

func (h *T) Written() uint64 { return uint64(h.off) }

func (h *T) Append(p []byte) (err error) {
	if len(h.buf)+len(p) > cap(h.buf) {
		size := h.cap
		if size > len(h.buf) {
			size = len(h.buf)
		}

		_, err = h.fh.Write(h.buf[:size])
		if err != nil {
			return err
		}

		h.buf = h.buf[:copy(h.buf, h.buf[size:]):cap(h.buf)]
	}

	h.buf = append(h.buf, p...)
	h.off += int64(len(p))
	return err
}

func (h *T) Flush() error {
	if len(h.buf) == 0 {
		return nil
	}

	if _, err := h.fh.Write(h.buf); err != nil {
		return err
	}

	h.buf = h.buf[:0]
	return nil
}

func (h *T) Sync() error {
	return h.fh.Sync()
}
