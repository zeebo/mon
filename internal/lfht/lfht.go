package lfht

import (
	"fmt"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/mon/internal/bitmap"
	"github.com/zeebo/xxh3"
)

// https://repositorio.inesctec.pt/bitstream/123456789/5465/1/P-00F-YAG.pdf

//
// parameters for the table
//

const (
	_width    = 3
	_entries  = 1 << _width
	_mask     = _entries - 1
	_bits     = bits.UintSize
	_depth    = 3
	_maxLevel = _bits / _width
)

//
// shorten some common phrases
//

type ptr = unsafe.Pointer

func cas(addr *ptr, old, new ptr) bool { return atomic.CompareAndSwapPointer(addr, old, new) }
func load(addr *ptr) ptr               { return atomic.LoadPointer(addr) }
func store(addr *ptr, val ptr)         { atomic.StorePointer(addr, val) }

func tag(b *Table) ptr   { return ptr(uintptr(ptr(b)) + 1) }
func tagged(p ptr) bool  { return uintptr(p)&1 > 0 }
func untag(p ptr) *Table { return (*Table)(ptr(uintptr(p) - 1)) }

//
// hashing support
//

func hash(x string) uintptr {
	return uintptr(xxh3.HashString(x))
}

//
// helper data types
//

type lazyValue struct {
	value ptr
	fn    func() ptr
}

func (lv *lazyValue) get() ptr {
	if lv.value == nil {
		lv.value = lv.fn()
	}
	return lv.value
}

type hashedKey struct {
	key  string
	hash uintptr
}

//
// data structrue
//

type tableHeader struct {
	level  uint
	prev   *Table
	bitmap bitmap.B128
}

type Table struct {
	tableHeader
	_       [64 - unsafe.Sizeof(tableHeader{})]byte // pad to cache line
	buckets [_entries]ptr
}

func (t *Table) getHashBucket(hash uintptr) (*ptr, uint) {
	idx := uint(hash>>((t.level*_width)&(_bits-1))) & _mask
	return &t.buckets[idx], idx
}

type node struct {
	key   string
	value ptr
	next  ptr
}

func (n *node) getNextRef() *ptr { return &n.next }

//
// upsert
//

func (t *Table) Upsert(k string, vf func() unsafe.Pointer) unsafe.Pointer {
	return t.upsert(hashedKey{key: k, hash: hash(k)}, lazyValue{fn: vf}).value
}

func (t *Table) upsert(key hashedKey, value lazyValue) *node {
	bucket, idx := t.getHashBucket(key.hash)
	entryRef := load(bucket)
	if entryRef == nil {
		newNode := &node{key: key.key, value: value.get(), next: tag(t)}
		if cas(bucket, nil, ptr(newNode)) {
			t.bitmap.Set(idx)
			return newNode
		}
		entryRef = load(bucket)
	}

	if tagged(entryRef) {
		return untag(entryRef).upsert(key, value)
	}
	return (*node)(entryRef).upsert(key, value, t, 1)
}

func (n *node) upsert(key hashedKey, value lazyValue, t *Table, count int) *node {
	if n.key == key.key {
		return n
	}

	next := n.getNextRef()
	nextRef := load(next)
	if nextRef == tag(t) {
		if count == _depth && t.level+1 < _maxLevel {
			newTable := &Table{tableHeader: tableHeader{
				level: t.level + 1,
				prev:  t,
			}}
			if cas(next, tag(t), tag(newTable)) {
				bucket, _ := t.getHashBucket(key.hash)
				adjustChainNodes((*node)(load(bucket)), newTable)
				store(bucket, tag(newTable))
				return newTable.upsert(key, value)
			}
		} else {
			newNode := &node{key: key.key, value: value.get(), next: tag(t)}
			if cas(next, tag(t), ptr(newNode)) {
				return newNode
			}
		}
		nextRef = load(next)
	}

	if tagged(nextRef) {
		prevTable := untag(nextRef)
		for prevTable.prev != nil && prevTable.prev != t {
			prevTable = prevTable.prev
		}
		return prevTable.upsert(key, value)
	}
	return (*node)(nextRef).upsert(key, value, t, count+1)
}

//
// adjust
//

func adjustChainNodes(r *node, t *Table) {
	next := r.getNextRef()
	nextRef := load(next)
	if nextRef != tag(t) {
		adjustChainNodes((*node)(nextRef), t)
	}
	t.adjustNode(r)
}

func (t *Table) adjustNode(n *node) {
	next := n.getNextRef()
	store(next, tag(t))

	bucket, idx := t.getHashBucket(hash(n.key))
	entryRef := load(bucket)
	if entryRef == nil {
		if cas(bucket, nil, ptr(n)) {
			t.bitmap.Set(idx)
			return
		}
		entryRef = load(bucket)
	}

	if tagged(entryRef) {
		untag(entryRef).adjustNode(n)
		return
	}
	n.adjustNode(t, (*node)(entryRef), 1)
}

func (n *node) adjustNode(t *Table, r *node, count int) {
	next := r.getNextRef()
	nextRef := load(next)
	if nextRef == tag(t) {
		if count == _depth && t.level+1 < _maxLevel {
			newTable := &Table{tableHeader: tableHeader{
				level: t.level + 1,
				prev:  t,
			}}
			if cas(next, tag(t), tag(newTable)) {
				bucket, _ := t.getHashBucket(hash(n.key))
				adjustChainNodes((*node)(load(bucket)), newTable)
				store(bucket, tag(newTable))
				newTable.adjustNode(n)
				return
			}
		} else if cas(next, tag(t), ptr(n)) {
			return
		}
		nextRef = load(next)
	}

	if tagged(nextRef) {
		prevTable := untag(nextRef)
		for prevTable.prev != nil && prevTable.prev != t {
			prevTable = prevTable.prev
		}
		prevTable.adjustNode(n)
		return
	}
	n.adjustNode(t, (*node)(nextRef), count+1)
}

//
// lookup
//

func (t *Table) Lookup(k string) unsafe.Pointer {
	return t.lookup(hashedKey{key: k, hash: hash(k)})
}

func (t *Table) lookup(key hashedKey) ptr {
	// if lookup misses are frequent, it may be worthwhile to check
	// the bitmap to avoid a cache miss loading the bucket.
	bucket, _ := t.getHashBucket(key.hash)
	entryRef := load(bucket)
	if entryRef == nil {
		return nil
	}
	if tagged(entryRef) {
		return untag(entryRef).lookup(key)
	}
	return (*node)(entryRef).lookup(key, t)
}

func (n *node) lookup(key hashedKey, t *Table) ptr {
	if n.key == key.key {
		return n.value
	}

	next := n.getNextRef()
	nextRef := load(next)
	if tagged(nextRef) {
		prevTable := untag(nextRef)
		for prevTable.prev != nil && prevTable.prev != t {
			prevTable = prevTable.prev
		}
		return prevTable.lookup(key)
	}
	return (*node)(nextRef).lookup(key, t)
}

//
// iterator
//

type Iterator struct {
	n     *node
	top   int
	stack [_maxLevel]struct {
		table *Table
		pos   bitmap.B128
	}
}

func (t *Table) Iterator() (itr Iterator) {
	itr.stack[0].table = t
	itr.stack[0].pos = t.bitmap.Clone()
	return itr
}

func (i *Iterator) Next() bool {
next:
	// if the stack is empty, we're done
	if i.top < 0 {
		return false
	}
	is := &i.stack[i.top]

	// if we don't have a node, load it from the top of the stack
	var nextTable *Table
	if i.n == nil {
		idx, ok := is.pos.Next()
		if !ok {
			// if we've walked the whole table, pop it and try again
			i.top--
			goto next
		}

		bucket := &is.table.buckets[idx&127]
		entryRef := load(bucket)

		// if it's a node, set it and continue
		if !tagged(entryRef) {
			i.n = (*node)(entryRef)
			return true
		}

		// otherwise, we need to walk to a new table.
		nextTable = untag(entryRef)
	} else {
		// if we have a node, try to walk to the next entry.
		nextRef := load(i.n.getNextRef())

		// if it's a node, set it and continue
		if !tagged(nextRef) {
			i.n = (*node)(nextRef)
			return true
		}

		// otherwise, we need to walk to a new table
		nextTable = untag(nextRef)
	}

	// if we're on the same table, just go to the next entry
	if nextTable == is.table {
		i.n = nil
		goto next
	}

	// walk nextTable backwards as much as possible.
	for nextTable.prev != nil && nextTable.prev != is.table {
		nextTable = nextTable.prev
	}

	// if it's a different table, push it on to the stack.
	if nextTable != is.table {
		i.top++
		i.stack[i.top].table = nextTable
		i.stack[i.top].pos = nextTable.bitmap.Clone()
	}

	// walk to the next entry in the top of the stack table
	i.n = nil
	goto next
}

func (i *Iterator) Key() string           { return i.n.key }
func (i *Iterator) Value() unsafe.Pointer { return i.n.value }

//
// dumping code
//

const dumpIndent = "|    "

func dumpPointer(indent string, p ptr) {
	if tagged(p) {
		table := untag(p)
		fmt.Printf("%stable[%p]:\n", indent, table)
		for i := range &table.buckets {
			dumpPointer(indent+dumpIndent, load(&table.buckets[i]))
		}
	} else if p != nil {
		n := (*node)(p)
		p := load(&n.next)
		fmt.Printf("%snode[%p](key:%q, value:%p, next:%p):\n", indent, n, n.key, n.value, p)
		if !tagged(p) {
			dumpPointer(indent+dumpIndent, load(&n.next))
		}
	}
}

func (t *Table) dump() { dumpPointer("", tag(t)) }
