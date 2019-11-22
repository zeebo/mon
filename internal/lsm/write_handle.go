package lsm

import (
	"sync/atomic"
	"time"
)

type writeHandle struct {
	fh  file
	off int64
	cap int
	buf []byte
}

func newWriteHandle(fh file, cap int) *writeHandle {
	var wh writeHandle
	initWriteHandle(&wh, fh, make([]byte, 0, cap))
	return &wh
}

func newWriteHandleBuf(fh file, buf []byte) *writeHandle {
	var wh writeHandle
	initWriteHandle(&wh, fh, buf[:0])
	return &wh
}

func initWriteHandle(wh *writeHandle, fh file, buf []byte) {
	wh.fh = fh
	wh.cap = cap(buf)
	wh.buf = buf
}

func (h *writeHandle) Written() uint64 { return uint64(h.off) }

func (h *writeHandle) Append(p []byte) (err error) {
	if len(h.buf)+len(p) > cap(h.buf) {
		size := h.cap
		if size > len(h.buf) {
			size = len(h.buf)
		}

		var s time.Time
		if trackStats {
			s = time.Now()
		}
		var n int
		n, err = h.fh.Write(h.buf[:size])
		if trackStats {
			atomic.AddInt64(&writtenDur, time.Since(s).Nanoseconds())
			atomic.AddInt64(&written, int64(n))
		}

		h.buf = h.buf[:copy(h.buf, h.buf[size:]):cap(h.buf)]
	}

	h.buf = append(h.buf, p...)
	h.off += int64(len(p))
	return err
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
