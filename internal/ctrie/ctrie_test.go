package ctrie

import (
	"testing"
	"unsafe"

	. "github.com/zeebo/mon/internal/tests"
)

func TestCtrie(t *testing.T) {
	var tr Tree
	for i := uint32(0); i < Size; i++ {
		tr.Upsert(Key(i), func() unsafe.Pointer { return Ptr(i) })
	}
	for i := uint32(0); i < Size; i++ {
		if tr.Lookup(Key(i)) != Ptr(i) {
			tr.dump()
			t.Fatal(i)
		}
	}
	for iter := tr.Iterator(); iter.Next(); {
		if tr.Lookup(iter.Key()) != iter.Value() {
			tr.dump()
			t.Fatal(iter.Key(), iter.Value())
		}
	}
}

func BenchmarkCtrie(b *testing.B) {
	RunBenchmarks(b, func() Type { return new(Tree) })

	b.Run("Iterate", func(b *testing.B) {
		var tr Tree
		for i := uint32(0); i < Size; i++ {
			tr.Upsert(Key(i), Empty)
		}
		b.ReportAllocs()
		b.ResetTimer()

		iter := tr.Iterator()
		for i := 0; i < b.N; i++ {
			if !iter.Next() {
				iter = tr.Iterator()
			}
		}
	})
}
