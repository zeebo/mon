package layermem

import (
	"math/bits"

	"github.com/zeebo/mon/internal/lsm/inlineptr"
)

const largestPrefix uint64 = 1<<64 - 1

type layerEntry struct {
	prefix uint64
	entry  uint32
}

type layer struct {
	n    int
	data [fanout][]layerEntry
}

func (l *layer) full() bool {
	return l.n >= fanout
}

func (l *layer) bump() {
	l.n++
}

func (l *layer) reset() {
	l.n = 0
	for i := range l.data {
		l.data[i] = l.data[i][:0]
	}
}

func (l *layer) merge(m *T, into []layerEntry) []layerEntry {
	data := l.data

	var heads [fanout]layerEntry
	for i, entries := range &data {
		if len(entries) > 0 {
			heads[i] = entries[0]
			data[i] = entries[1:]
		} else {
			heads[i].prefix = largestPrefix
		}
	}

	for {
		// fmt.Printf("%x\n", heads)

		first, second := min2Entries4(&heads)
		first %= fanout

		ent := heads[first]
		if pre := ent.prefix; pre == largestPrefix {
			return into
		} else if bits.TrailingZeros64(pre) < 8 && pre == heads[second%fanout].prefix {
			first = findSmallest(m, first, heads) % fanout
			ent = heads[first]
		}

		into = append(into, ent)

		if entries := data[first]; len(entries) > 0 {
			heads[first] = entries[0]
			data[first] = entries[1:]
		} else {
			heads[first].prefix = largestPrefix
		}
	}
}

func findSmallest(m *T, first uint8, heads [fanout]layerEntry) uint8 {
	kptr0 := m.ents[heads[0].entry].Key()
	kptr1 := m.ents[heads[1].entry].Key()
	kptr2 := m.ents[heads[2].entry].Key()
	kptr3 := m.ents[heads[3].entry].Key()

	var ks [4][]byte

	switch kptr0[0] {
	case inlineptr.Inline:
		ks[0] = kptr0.InlineData()
	case inlineptr.Pointer:
		offset := kptr0.Offset()
		ks[0] = m.data[offset:][:kptr0.Length()]
	}

	switch kptr1[0] {
	case inlineptr.Inline:
		ks[1] = kptr1.InlineData()
	case inlineptr.Pointer:
		offset := kptr1.Offset()
		ks[1] = m.data[offset:][:kptr1.Length()]
	}

	switch kptr2[0] {
	case inlineptr.Inline:
		ks[2] = kptr2.InlineData()
	case inlineptr.Pointer:
		offset := kptr2.Offset()
		ks[2] = m.data[offset:][:kptr2.Length()]
	}

	switch kptr3[0] {
	case inlineptr.Inline:
		ks[3] = kptr3.InlineData()
	case inlineptr.Pointer:
		offset := kptr3.Offset()
		ks[3] = m.data[offset:][:kptr3.Length()]
	}

	i := first

	if heads[0].prefix != largestPrefix && string(ks[0]) < string(ks[i]) {
		i = 0
	}
	if heads[1].prefix != largestPrefix && string(ks[1]) < string(ks[i]) {
		i = 1
	}
	if heads[2].prefix != largestPrefix && string(ks[2]) < string(ks[i]) {
		i = 2
	}
	if heads[3].prefix != largestPrefix && string(ks[3]) < string(ks[i]) {
		i = 3
	}

	return uint8(i)
}
