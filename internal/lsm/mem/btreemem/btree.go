package btreemem

import (
	"bytes"

	"github.com/zeebo/mon/internal/lsm/inlineptr"
)

type btreeEntry struct {
	kptr  inlineptr.T
	val   uint32
	pivot uint32
}

// compare is like bytes.Compare but for uint64s.
func compare(a, b uint64) int {
	if a == b {
		return 0
	} else if a < b {
		return -1
	}
	return 1
}

// btree is an in memory B+ tree tuned to store entries
type btree struct {
	root  *btreeNode
	rid   uint32
	count uint32
	nodes []*btreeNode
}

// Reset clears the btree back to an empty state
func (b *btree) Reset() {
	b.root.reset()
	b.rid = 0
	b.count = 0
	b.nodes = b.nodes[:0]
}

// search returns the leaf node that should contain the key.
func (b *btree) search(ent btreeEntry, key, buf []byte) (*btreeNode, uint32) {
	prefix := ent.kptr.Prefix()
	n, nid := b.root, b.rid

	for !n.leaf {
		// binary search to find the appropriate child
		i, j := uint16(0), n.count
		for i < j {
			h := (i + j) >> 1
			khptr := n.payload[h].kptr
			prefixh := khptr.Prefix()

			// first, check the saved prefix. this avoids having to hop and
			// read the key if one is different from the other.
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

				if bytes.Compare(key, kh) >= 0 {
					i = h + 1
				} else {
					j = h
				}
			case -1:
				j = h
			}
		}

		if i == n.count {
			nid = n.next
		} else {
			nid = n.payload[i].pivot
		}
		n = b.nodes[nid]
	}

	return n, nid
}

// alloc creates a fresh node.
func (b *btree) alloc(leaf bool) (n *btreeNode, id uint32) {
	if len(b.nodes) < cap(b.nodes) {
		n = b.nodes[:len(b.nodes)+1][len(b.nodes)]
		n.reset()
	}
	if n == nil {
		n = new(btreeNode)
	}

	n.next = invalidNode
	n.prev = invalidNode
	n.parent = invalidNode
	n.leaf = leaf
	n.ok = true
	b.nodes = append(b.nodes, n)

	return n, uint32(len(b.nodes) - 1)
}

// split the node in half, returning a new node containing the
// smaller half of the keys.
func (b *btree) split(n *btreeNode, nid uint32) (*btreeNode, uint32) {
	s, sid := b.alloc(n.leaf)
	s.parent = n.parent

	// split the entries between the two nodes
	s.count = uint16(copy(s.payload[:], n.payload[:payloadSplit]))

	copyAt := payloadSplit
	if !n.leaf {
		// if it's not a leaf, we don't want to include the split btreeEntry
		copyAt++

		// additionally, the next pointer should be what the split btreeEntry
		// points at.
		s.next = n.payload[payloadSplit].pivot

		// additionally, every element that it points at needs to have
		// their parent updated
		b.nodes[s.next].parent = sid
		for i := uint16(0); i < s.count; i++ {
			b.nodes[s.payload[i].pivot].parent = sid
		}
	} else {
		// if it is a leaf, fix up the next and previous pointers
		s.next = nid
		if n.prev != invalidNode {
			s.prev = n.prev
			b.nodes[s.prev].next = sid
		}
		n.prev = sid
	}
	n.count = uint16(copy(n.payload[:], n.payload[copyAt:]))

	return s, sid
}

// Insert puts the btreeEntry into the btree, using the buf to read keys
// to determine the position. It returns true if the insert created
// a new btreeEntry.
func (b *btree) Insert(kptr inlineptr.T, val uint32, key, buf []byte) bool {
	ent := btreeEntry{kptr: kptr, val: val}

	// easy case: if we have no root, we can just allocate it
	// and insert the btreeEntry.
	if b.root == nil || !b.root.ok {
		b.root, b.rid = b.alloc(true)
		b.root.insertEntry(ent, key, buf)
		b.count++
		return true
	}

	// search for the leaf that should contain the node
	n, nid := b.search(ent, key, buf)
	for {
		added := n.insertEntry(ent, key, buf)
		if added && n.leaf {
			b.count++
		}

		// easy case: if the node still has enough room, we're done.
		if n.count < payloadEntries {
			return added
		}

		// update the btreeEntry we're going to insert to be the btreeEntry we're
		// splitting the node on.
		ent = n.payload[payloadSplit]

		// split the node. s is a new node that contains keys
		// smaller than the splitbtreeEntry.
		s, sid := b.split(n, nid)

		// find the parent, allocating a new node if we're looking
		// at the root, and set the parent of the split node.
		var p *btreeNode
		var pid uint32
		if n.parent != invalidNode {
			p, pid = b.nodes[n.parent], n.parent
		} else {
			// create a new parent node, and make it point at the
			// larger side of the split node for it's next pointer.
			p, pid = b.alloc(false)
			p.next = nid
			n.parent = pid
			s.parent = pid

			// store it as the root
			b.root, b.rid = p, pid
		}

		// make a pointer out of the split btreeEntry to point at the
		// newly split node, and try to insert it.
		ent.pivot = sid
		n, nid = p, pid
	}
}

func (b *btree) Iterator(buf []byte, vptrs []inlineptr.T) Iterator {
	// find the deepest leftmost node
	n := b.root
	if n == nil {
		return Iterator{}
	}

	for !n.leaf {
		nid := n.payload[0].pivot
		if n.count == 0 {
			nid = n.next
		}
		n = b.nodes[nid]
	}

	return Iterator{
		b:     b,
		n:     n,
		i:     uint16(1<<16 - 1), // overflow hack. this is -1
		buf:   buf,
		vptrs: vptrs,
	}
}
