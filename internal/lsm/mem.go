package lsm

import (
	"sort"
)

type mem struct {
	cap     uint64
	entries map[string]entry
	data    []byte
}

func newMem(cap uint64) mem {
	return mem{
		cap:     cap,
		entries: make(map[string]entry),
		data:    make([]byte, 0, cap),
	}
}

func (m *mem) Len() uint64 { return uint64(len(m.data)) }
func (m *mem) Cap() uint64 { return m.cap }

func (m *mem) SetString(key string, value []byte) bool {
	kptr := newInlinePtrString(key)
	if kptr.Pointer() {
		kptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, key...)
	}
	vptr := newInlinePtrBytes(value)
	if vptr.Pointer() {
		vptr.SetOffset(uint64(len(m.data)))
		m.data = append(m.data, value...)
	}

	m.entries[key] = newEntry(kptr, vptr)

	return 32*uint64(len(m.entries))+uint64(len(m.data)) < m.cap
}

func (m *mem) Iterator() mergeIter {
	sorter := &memSorter{
		data:     m.data,
		entries:  make([]entry, 0, len(m.entries)),
		prefixes: make([]uint64, 0, len(m.entries)),
	}

	for _, ent := range m.entries {
		sorter.entries = append(sorter.entries, ent)
		sorter.prefixes = append(sorter.prefixes, ent.Key().Prefix())
	}

	sort.Sort(sorter)

	// TODO(jeff): should be an iterator
	return nil
}

type memSorter struct {
	data     []byte
	entries  []entry
	prefixes []uint64
}

func (m *memSorter) Len() int { return len(m.entries) }

func (m *memSorter) Swap(i int, j int) {
	swapEntries(m.entries, i, j)
	swapPrefixes(m.prefixes, i, j)
}

// this is weird but the Swap method compiles WAY worse with direct field
// access for the swap bit. if we pass it through an inlined function, it
// generates much better code. i have no idea.

func swapEntries(xs []entry, i, j int)   { xs[i], xs[j] = xs[j], xs[i] }
func swapPrefixes(xs []uint64, i, j int) { xs[i], xs[j] = xs[j], xs[i] }

func (m *memSorter) Less(i int, j int) bool {
	kip, kjp := m.prefixes[i], m.prefixes[j]
	if kip < kjp {
		return true
	} else if kip > kjp {
		return false
	}

	ki, kj := m.entries[i].Key(), m.entries[j].Key()
	kis, kjs := ki.Offset(), kj.Offset()
	kie, kje := kis+uint64(ki.Length()), kjs+uint64(kj.Length())

	return string(m.data[kis:kie]) < string(m.data[kjs:kje])
}
