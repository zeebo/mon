package ctrie

import (
	"fmt"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash"
)

//
// shorten some common phrases
//

type ptr = unsafe.Pointer

func cas(addr *ptr, old, new ptr) bool { return atomic.CompareAndSwapPointer(addr, old, new) }
func load(addr *ptr) ptr               { return atomic.LoadPointer(addr) }
func store(addr *ptr, val ptr)         { atomic.StorePointer(addr, val) }

//
// hashing support
//

func hash(x string) uintptr { return uintptr(xxhash.Sum64String(x)) }

//
// bitmap support
//

type bitmap = uint32

const (
	_width = 5
	_size  = 1 << _width
	_mask  = _size - 1
	_bits  = bits.UintSize
	_depth = _bits / _width
)

func getPos(h uintptr, lev uint) uint {
	return uint(h>>(lev&(_bits-1))) & _mask
}

//
// inode + snode + lnode
//

var cnodeKind = &inode{key: "cnodeKind"}

type inode struct {
	key   string
	value ptr
	next  *inode
}

func (i *inode) loadI() *inode                { return (*inode)(load(&i.value)) }
func (i *inode) updateI(old, new *inode) bool { return cas(&i.value, ptr(old), ptr(new)) }
func (i *inode) loadC() *cnode                { return (*cnode)(load(&i.value)) }
func (i *inode) updateC(old, new *cnode) bool { return cas(&i.value, ptr(old), ptr(new)) }

//
// cnode
//

type cnode struct {
	bitmap bitmap
	array  [_size]*inode
}

func (c cnode) set(pos uint, i *inode) *cnode {
	c.array[pos&_mask] = i
	c.bitmap |= 1 << pos
	return &c
}

//
// tree
//

type Tree struct{ root *inode }

func (t *Tree) addr() *ptr   { return (*ptr)(ptr(&t.root)) }
func (t *Tree) load() *inode { return (*inode)(load(t.addr())) }

// Lookup reports the pointer associated with the key, or nil.
func (t *Tree) Lookup(k string) unsafe.Pointer {
	i, lev, h := t.load(), uint(0), hash(k)
	if i == nil {
		cas(t.addr(), nil, ptr(&inode{value: ptr(new(cnode)), next: cnodeKind}))
		i = t.load()
	}

nextNode:
	if i.next == cnodeKind {
		cn := i.loadC()
		pos := getPos(h, lev)
		i = cn.array[pos]

		// if it's not in the array, it's not present.
		if i == nil {
			return nil
		}

		// try again at the next inode
		lev += _width
		goto nextNode
	}

	// at this point, i must be an snode/lnode. walk the list.
nextLnode:
	if i.key == k {
		return i.value
	}
	i = i.next
	if i == nil {
		return nil
	}
	goto nextLnode
}

// Upsert either reports the pointer associated with the key, or calls the provided
// function to allocate a pointer, attempts to store it, and reports whatever pointer
// is now associated with the key. It calls the function at most once.
func (t *Tree) Upsert(k string, vf func() unsafe.Pointer) unsafe.Pointer {
	i, lev, h, v := t.load(), uint(0), hash(k), ptr(nil)
	if i == nil {
		cas(t.addr(), nil, ptr(&inode{value: ptr(new(cnode)), next: cnodeKind}))
		i = t.load()
	}

again:
	// if it's an lnode or snode, extend it.
	if i.next != cnodeKind {
		ln := i.loadI()
		if v == nil {
			v = vf()
		}
		if i.updateI(ln, &inode{key: k, value: v, next: ln}) {
			return v
		}
		goto again
	}

	// it must be a cnode. determine where the key fits in it.
	cn := i.loadC()
	pos := getPos(h, lev)
	sn := cn.array[pos]

	// if there's no entry in the array, insert it.
	if sn == nil {
		if v == nil {
			v = vf()
		}
		if i.updateC(cn, cn.set(pos, &inode{key: k, value: v})) {
			return v
		}
		goto again
	}

	// if it's an inode, loop again.
	if sn.next == cnodeKind {
		i, lev = sn, lev+_width
		goto again
	}

	// if the keys match, then leave the old one.
	if sn.key == k {
		return sn.value
	}

	// we have a hash collision at this level. if we can't go to another level, then
	// create an lnode.
	if lev+_width > _bits {
		if v == nil {
			v = vf()
		}
		if i.updateC(cn, cn.set(pos, &inode{key: k, value: v, next: sn})) {
			return v
		}
		goto again
	}

	// since we can go deeper, convert the snode at this location into a cnode containing
	// only the snode.
	snPos := getPos(hash(sn.key), lev+_width)
	cnn := new(cnode)
	cnn.array[snPos] = sn
	in := &inode{value: ptr(cnn), next: cnodeKind}
	if !i.updateC(cn, cn.set(pos, in)) {
		goto again
	}

	// if the update worked, then we loop again on that new inode.
	i, lev = in, lev+_width
	goto again
}

//
// iterator
//

type Iterator struct {
	in    *inode
	top   int
	stack [_depth]struct {
		cn  *cnode
		pos bitmap
	}
}

func (t *Tree) Iterator() (itr Iterator) {
	if i := t.load(); i == nil {
		itr.top = -1
	} else {
		itr.stack[0].cn = i.loadC()
	}
	return itr
}

func (i *Iterator) Next() bool {
	// walk the inode chain
	if i.in != nil && i.in.next != nil {
		i.in = i.in.next
		return true
	}

nextPos:
	// if we popped past the top, we're done.
	if i.top < 0 {
		return false
	}
	is := &i.stack[i.top]

	// if there aren't any bits left, pop the stack.
	unused := is.pos ^ is.cn.bitmap
	if unused == 0 {
		i.top--
		goto nextPos
	}

	// update the position with the highest bit and access it.
	idx := bitmap(bits.Len32(unused)-1) & _mask
	is.pos |= 1 << idx
	i.in = is.cn.array[idx]

	// if it's actually an snode, we're done.
	if i.in.next != cnodeKind {
		return true
	}

	// otherwise we have to push the cnode onto the iteration stack
	i.top++
	i.stack[i.top].cn = i.in.loadC()
	i.stack[i.top].pos = 0
	goto nextPos
}

func (i *Iterator) Key() string           { return i.in.key }
func (i *Iterator) Value() unsafe.Pointer { return i.in.value }

//
// dumping code
//

const dumpIndent = "|    "

func (i *inode) dump(indent string) {
	if i.next == cnodeKind {
		fmt.Printf("%sinode[%p]:\n", indent, i)
		i.loadC().dump(indent + dumpIndent)
	} else {
		fmt.Printf("%ssnode[%p](key:%q value:%p)\n", indent, i, i.key, i.value)
		if i.next != nil {
			i.next.dump(indent)
		}
	}
}

func (c *cnode) dump(indent string) {
	fmt.Printf("%scnode[%p]:\n", indent, c)
	indent += dumpIndent
	for _, i := range &c.array {
		if i != nil {
			i.dump(indent)
		}
	}
}

func (t *Tree) dump() {
	fmt.Printf("tree[%p]:\n", t)
	t.root.dump(dumpIndent)
}
