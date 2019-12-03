package lsm

import (
	"unsafe"

	"github.com/zeebo/errs"
)

type iterator interface {
	Next() bool

	Entry() entry

	Key() []byte
	Value() []byte // Key is no longer valid after a call to Value until Next

	Err() error
}

const bufferSize = 64 * 1024

type entry [2 * inlinePtrSize]byte

const entrySize = 32

func newEntry(kptr, vptr inlinePtr) (ent entry) {
	copy(ent[0:inlinePtrSize], kptr[:])
	copy(ent[inlinePtrSize:2*inlinePtrSize], vptr[:])
	return ent
}

func (e *entry) Key() *inlinePtr   { return (*inlinePtr)(unsafe.Pointer(&e[0])) }
func (e *entry) Value() *inlinePtr { return (*inlinePtr)(unsafe.Pointer(&e[inlinePtrSize])) }

type cleaner []func() error

func (c *cleaner) Add(cl func() error) { *c = append(*c, cl) }

func (c *cleaner) Close(err *error) {
	if err != nil && *err != nil {
		for i := len(*c) - 1; i >= 0; i-- {
			*err = errs.Combine(*err, (*c)[i]())
		}
	}
}
