package flat

import (
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/xxh3"
)

// TODO: this doesn't work if the hash table runs out of space. a high load factor is
// terrible for it, since it's linear probing.

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

func hash(x string) uintptr {
	// tag the high bit on every hash so that we can't get zero.
	return uintptr(xxh3.HashString(x)) | uintptr(1<<(bits.UintSize-1))
}

const (
	_width   = 16
	_entries = 1 << _width
	_mask    = _entries - 1
)

type Table struct {
	entries [_entries]entry
}

type entry struct {
	hash  uintptr
	key   string
	value ptr
}

func (e *entry) claim(h uintptr, k string, v ptr) bool {
	// non-nil value claims the entry
	if !cas(&e.value, nil, v) {
		return false
	}

	// storing the hash atomically ensures key is written fully
	e.key = k
	atomic.StoreUintptr(&e.hash, h)

	return true
}

func (t *Table) Upsert(k string, vf func() unsafe.Pointer) unsafe.Pointer {
	h := hash(k)
again:
	entry := &t.entries[h&_mask]
	if eh := atomic.LoadUintptr(&entry.hash); eh == 0 {
		if v := vf(); entry.claim(h, k, vf()) {
			return v
		}
	} else if eh == h && entry.key == k {
		return entry.value
	}
	h++
	goto again
}

func (t *Table) Lookup(k string) unsafe.Pointer {
	h := hash(k)
again:
	entry := &t.entries[h&_mask]
	if eh := atomic.LoadUintptr(&entry.hash); eh == 0 {
		return nil // probing finished
	} else if eh == h && entry.key == k {
		return entry.value
	}
	h++
	goto again
}
