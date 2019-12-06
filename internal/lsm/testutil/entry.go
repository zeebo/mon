package testutil

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
	"github.com/zeebo/pcg"
)

func MakeEntry(key, value string) entry.T {
	return entry.New(inlineptr.String(key), inlineptr.String(value))
}

func RandomEntry() entry.T {
	return MakeEntry(fmt.Sprint(pcg.Uint32()), fmt.Sprint(pcg.Uint32()))
}

type FakeIterator []entry.T

func (f *FakeIterator) Err() error     { return nil }
func (f *FakeIterator) Entry() entry.T { return (*f)[0] }
func (f *FakeIterator) Key() []byte    { return (*f)[0].Key().InlineData() }
func (f *FakeIterator) Value() []byte  { return (*f)[0].Value().InlineData() }

func (f *FakeIterator) Next() bool {
	if len(*f) > 0 {
		*f = (*f)[1:]
	}
	return len(*f) > 0
}

func NewFakeIterator(value string, keys ...string) *FakeIterator {
	out := FakeIterator{entry.T{}}
	for _, key := range keys {
		out = append(out, MakeEntry(key, value))
	}
	return &out
}

func NewRandomFakeIterator(count int) *FakeIterator {
	out := FakeIterator{entry.T{}}
	for i := 0; i < count; i++ {
		out = append(out, RandomEntry())
	}
	sortEntries([]entry.T(out[1:]))
	return &out
}

func sortEntries(ents []entry.T) {
	sort.Slice(ents, func(i, j int) bool {
		return bytes.Compare(ents[i].Key().InlineData(), ents[j].Key().InlineData()) < 0
	})
}
