package lsm

import (
	"sort"
)

type mem struct {
	len  uint64
	cap  uint64
	sets map[string][]byte
	dels map[string]struct{}
}

func newMem(cap uint64) mem {
	return mem{
		len:  headerSize,
		cap:  cap,
		sets: make(map[string][]byte),
		dels: make(map[string]struct{}),
	}
}

func (m *mem) Len() uint64 { return m.len }
func (m *mem) Cap() uint64 { return m.cap }

func (m *mem) SetString(key string, value []byte) bool {
	if value == nil {
		value = []byte{}
	}

	if len(m.dels) == 0 {
		if _, ok := m.sets[key]; !ok {
			m.len += entrySize + uint64(len(key))
		}
	} else {
		if _, ok := m.dels[key]; ok {
			delete(m.dels, key)
		} else {
			m.len += entrySize + uint64(len(key))
		}
	}
	m.sets[key] = value
	return m.len < m.cap
}

func (m *mem) AppendTo(buf []byte) []byte {
	out := buf
	if uint64(cap(out)-len(out)) < m.len {
		out = make([]byte, 0, m.len)
	}

	numEntries := uint64(len(m.sets)) + uint64(len(m.dels))

	{
		h := newHeader(0, headerSize, numEntries, 0)
		out = append(out, h[:]...)
	}

	// TODO(jeff): man this sorting is prolly gonna be slow
	memKeys := make([]memKeyState, 0, numEntries)
	for key, value := range m.sets {
		kptr := newInlinePtrString(key)
		memKeys = append(memKeys, memKeyState{
			prefix: kptr.Prefix(),
			key:    key,
			value:  value,
			ent:    newEntry(kptr, newInlinePtrBytes(value)),
		})
	}
	for key := range m.dels {
		kptr := newInlinePtrString(key)
		memKeys = append(memKeys, memKeyState{
			prefix: kptr.Prefix(),
			key:    key,
			ent:    newEntry(kptr, inlinePtr{}),
		})
	}
	sort.Sort(memKeyStates(memKeys))

	offset := headerSize + numEntries*entrySize
	for _, mk := range memKeys {
		if mk.ent.Key().Pointer() {
			mk.ent.Key().SetOffset(offset)
			offset += uint64(mk.ent.Key().Length())
		}
		if mk.ent.Value().Pointer() {
			mk.ent.Value().SetOffset(offset)
			offset += uint64(mk.ent.Value().Length())
		}

		out = append(out, mk.ent[:]...)
	}

	for _, mk := range memKeys {
		if mk.ent.Key().Pointer() {
			out = append(out, mk.key...)
		}
		if mk.ent.Value().Pointer() {
			out = append(out, mk.value...)
		}
	}

	return out
}

type memKeyState struct {
	prefix uint64
	key    string
	value  []byte
	ent    entry
}

type memKeyStates []memKeyState

func (ks memKeyStates) Len() int          { return len(ks) }
func (ks memKeyStates) Swap(i int, j int) { ks[i], ks[j] = ks[j], ks[i] }
func (ks memKeyStates) Less(i int, j int) bool {
	ki, kj := &ks[i], &ks[j]
	return ki.prefix < kj.prefix || (ki.prefix == kj.prefix && ki.key < kj.key)
}
