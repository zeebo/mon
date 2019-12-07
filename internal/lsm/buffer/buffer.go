package buffer

import (
	"github.com/zeebo/mon/internal/lsm/file"
)

const Size = 256 * 1024

type T struct {
	err error
	fh  file.T
	buf []byte
	n   int
}

func (b *T) Init(fh file.T, size int) {
	b.fh = fh
	if cap(b.buf) < size {
		b.buf = make([]byte, 0, size)
	} else {
		b.buf = b.buf[:size:size]
	}
}

func (b *T) Err() error    { return b.err }
func (b *T) Buffered() int { return len(b.buf) - b.n }

func (b *T) Read(n int) (data []byte, ok bool) {
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
		nn, err = b.fh.Read(buf[m:])
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
