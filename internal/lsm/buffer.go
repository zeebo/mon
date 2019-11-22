package lsm

import (
	"sync/atomic"
	"time"
)

type buffer struct {
	err error
	fh  file
	buf []byte
	n   int
}

func newBuffer(fh file, size int) *buffer {
	var buf buffer
	initBuffer(&buf, fh, size)
	return &buf
}

func initBuffer(buf *buffer, fh file, size int) {
	buf.fh = fh
	buf.buf = make([]byte, 0, size)
}

func (b *buffer) Err() error    { return b.err }
func (b *buffer) Buffered() int { return len(b.buf) - b.n }

func (b *buffer) Read(n int) (data []byte, ok bool) {
	if b.err != nil {
		return nil, false
	}

	if uint(b.n+n) <= uint(len(b.buf)) && uint(b.n) <= uint(b.n+n) {
		data = b.buf[b.n : b.n+n]
		b.n += n
		return data, true
	}

	buf := b.buf[:cap(b.buf)]
	if uint(n) > uint(len(buf)) {
		buf = make([]byte, n)
	}

	var err error
	var nn int

	m := uint(copy(buf, b.buf[b.n:]))
	for m < uint(len(buf)) && err == nil {
		var s time.Time
		if trackStats {
			s = time.Now()
		}
		nn, err = b.fh.Read(buf[m:])
		if trackStats {
			atomic.AddInt64(&readDur, time.Since(s).Nanoseconds())
			atomic.AddInt64(&read, int64(nn))
		}
		m += uint(nn)
	}

	if uint(n) <= uint(m) && uint(n) <= uint(len(buf)) {
		b.buf, data = buf[:m], buf[:n]
		b.n = n
		return data, true
	}

	if m == 0 {
		err = nil
	}
	b.err = err
	return nil, false
}
