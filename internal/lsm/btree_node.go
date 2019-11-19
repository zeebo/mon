package lsm

import (
	"bytes"
	"math"
)

const (
	invalidNode    = math.MaxUint32
	payloadEntries = 127
	payloadSplit   = payloadEntries / 2
)

// btreeNode are nodes in the btree.
type btreeNode struct {
	next    uint32 // pointer to the next node (or if not leaf, the rightmost edge)
	prev    uint32 // backpointer from next node (unused if not leaf)
	parent  uint32 // set to invalidNode on the root node
	count   uint16 // used values in payload
	leaf    bool   // set if is a leaf
	ok      bool
	payload [payloadEntries]btreeEntry
}

func (b *btreeNode) reset() {
	if b != nil {
		b.next = 0
		b.prev = 0
		b.parent = 0
		b.count = 0
		b.leaf = false
		b.ok = false
	}
}

// insertEntry inserts the entry into the node. it should never be called
// on a node that would have to split. it returns true if the count increased.
func (n *btreeNode) insertEntry(ent btreeEntry, key, buf []byte) bool {
	prefix := ent.kptr.Prefix()

	// binary search to find the appropriate child
	i, j := uint16(0), n.count
	for i < j {
		h := (i + j) >> 1
		enth := n.payload[h]
		khptr := enth.kptr
		prefixh := khptr.Prefix()

		switch compare(prefix, prefixh) {
		case 1:
			i = h + 1

		case 0:
			var kh []byte
			if khptr.Inline() {
				kh = khptr.InlineData()
			} else {
				begin := khptr.Offset()
				end := khptr.Offset() + uint64(khptr.Length())
				kh = buf[begin:end]
			}

			switch bytes.Compare(key, kh) {
			case 1:
				i = h + 1

			case 0:
				// found a match. overwite and exit.
				// we want to retain the pivot field, though.
				ent.pivot = enth.pivot
				n.payload[h] = ent
				return false

			case -1:
				j = h
			}

		case -1:
			j = h
		}
	}

	copy(n.payload[i+1:], n.payload[i:n.count])
	n.payload[i] = ent
	n.count++
	return true
}

// appendEntry appends the entry into the node. it must compare greater than any
// element inside of the node, already, and should never be called on a node that
// would have to split.
func (n *btreeNode) appendEntry(ent btreeEntry) {
	n.payload[n.count] = ent
	n.count++
}
