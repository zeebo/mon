package entry

import (
	"unsafe"

	"github.com/zeebo/mon/internal/lsm/inlineptr"
)

type T [2 * inlineptr.Size]byte

const Size = 32

func New(kptr, vptr inlineptr.T) (ent T) {
	copy(ent[0:inlineptr.Size], kptr[:])
	copy(ent[inlineptr.Size:2*inlineptr.Size], vptr[:])
	return ent
}

func (e *T) Key() *inlineptr.T   { return (*inlineptr.T)(unsafe.Pointer(&e[0])) }
func (e *T) Value() *inlineptr.T { return (*inlineptr.T)(unsafe.Pointer(&e[inlineptr.Size])) }
