package lsm

import (
	"bufio"
	"io"
	"unsafe"

	"github.com/zeebo/errs"
)

type entry [32]byte

const entrySize = 32

func newEntry(kptr, vptr inlinePtr) (ent entry) {
	copy(ent[0:16], kptr[:])
	copy(ent[16:32], vptr[:])
	return ent
}

func (e *entry) Key() *inlinePtr   { return (*inlinePtr)(unsafe.Pointer(&e[0])) }
func (e *entry) Value() *inlinePtr { return (*inlinePtr)(unsafe.Pointer(&e[16])) }

type buffer struct {
	br   *bufio.Reader
	read int64
	pref bool
	err  error
}

func newBufferSize(r io.Reader, size int) buffer {
	return buffer{
		br: bufio.NewReaderSize(r, size),
	}
}

func (b *buffer) Consumed() (int64, bool) {
	return b.read, b.pref
}

func (b *buffer) Read(n int) (data []byte, ok bool) {
	if b.err != nil {
		return nil, false
	}

	data, b.err = b.br.Peek(n)
	if b.err == nil {
		return data, true
	} else if len(data) > 0 && b.err == io.EOF {
		b.err = io.ErrUnexpectedEOF
		b.pref = true
	}

	return nil, false
}

func (b *buffer) Error() error {
	return b.err
}

func (b *buffer) Consume(n int) bool {
	if b.err != nil {
		return false
	}

	var nn int
	nn, b.err = b.br.Discard(n)
	b.read += int64(nn)

	if b.err != nil {
		return false
	} else if nn != n {
		b.err = errs.New("invalid discard")
		return false
	}

	return true
}

type cleaner []func() error

func (c *cleaner) Add(cl func() error) { *c = append(*c, cl) }

func (c *cleaner) Close(err *error) {
	if err != nil && *err != nil {
		for i := len(*c) - 1; i >= 0; i-- {
			*err = errs.Combine(*err, (*c)[i]())
		}
	}
}
