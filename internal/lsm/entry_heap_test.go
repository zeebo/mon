package lsm

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestEntryHeap(t *testing.T) {
	mk := func(key string) entryHeapElement { return entryHeapElement{mkey: []byte(key)} }
	mks := func(keys ...string) (out []entryHeapElement) {
		for _, key := range keys {
			out = append(out, mk(key))
		}
		return out
	}

	t.Run("New", func(t *testing.T) {
		expected := "abcde"
		eh := newEntryHeap(mks("c", "a", "e", "d", "b"))

		for i := 0; i < len(expected); i++ {
			ele, ok := eh.Pop()
			assert.That(t, ok)
			assert.Equal(t, string(ele.testKey()), expected[i:i+1])
		}
		_, ok := eh.Pop()
		assert.That(t, !ok)
	})

	t.Run("Push", func(t *testing.T) {
		expected := "abcde"
		eh := newEntryHeap(mks())
		eh.Push(mk("c"))
		eh.Push(mk("a"))
		eh.Push(mk("e"))
		eh.Push(mk("b"))
		eh.Push(mk("d"))

		for i := 0; i < len(expected); i++ {
			ele, ok := eh.Pop()
			assert.That(t, ok)
			assert.Equal(t, string(ele.testKey()), expected[i:i+1])
		}
		_, ok := eh.Pop()
		assert.That(t, !ok)
	})

	t.Run("Fuzz", func(t *testing.T) {
		const (
			numElements = 10
			numInitial  = 100
			numIters    = 1000
		)

		var exp []string
		rng := pcg.New(uint64(time.Now().UnixNano()))
		random := func() string { return fmt.Sprint(rng.Uint32n(numElements)) }

		for i, j := uint32(0), rng.Uint32n(numInitial); i < j; i++ {
			exp = append(exp, random())
		}

		eh := newEntryHeap(mks(exp...))
		sort.Strings(exp)

		for i := 0; i < numIters; i++ {
			if rng.Uint32()%2 == 0 {
				ele, ok := eh.Pop()
				assert.Equal(t, ok, len(exp) > 0)
				if ok {
					assert.Equal(t, string(ele.testKey()), exp[0])
					exp = exp[1:]
				}
			} else {
				s := random()
				exp = append(exp, s)
				sort.Strings(exp)
				eh.Push(mk(s))
			}
		}
	})
}

func BenchmarkEntryHeap(b *testing.B) {
	eles := [...]entryHeapElement{
		entryHeapElement{mkey: []byte("0")},
		entryHeapElement{mkey: []byte("1")},
		entryHeapElement{mkey: []byte("2")},
		entryHeapElement{mkey: []byte("3")},
		entryHeapElement{mkey: []byte("4")},
		entryHeapElement{mkey: []byte("5")},
		entryHeapElement{mkey: []byte("6")},
		entryHeapElement{mkey: []byte("7")},
	}

	b.Run("PopPush", func(b *testing.B) {
		var rng pcg.T
		eh := newEntryHeap(append([]entryHeapElement(nil), eles[:]...))

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			eh.Pop()
			eh.Push(eles[rng.Uint32()%8])
		}
	})
}
