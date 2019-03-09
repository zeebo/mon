package ctrie

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/zeebo/pcg"
)

func TestCtrie(t *testing.T) {
	const max = 10000

	var tr Tree
	for i := 0; i < max; i++ {
		tr.Upsert(ikey(i), func() unsafe.Pointer { return iptr(i) })
	}
	for i := 0; i < max; i++ {
		if tr.Lookup(ikey(i)) != iptr(i) {
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
	b.Run("Upsert", func(b *testing.B) {
		var tr Tree
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			tr.Upsert(ikey(int(pcg.Uint32n(10000))), nil)
		}
	})

	b.Run("Lookup", func(b *testing.B) {
		var sink unsafe.Pointer
		var tr Tree
		for i := 0; i < 10000; i++ {
			tr.Upsert(ikey(i), nil)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			sink = tr.Lookup(ikey(int(pcg.Uint32n(10000))))
		}

		runtime.KeepAlive(sink)
	})

	b.Run("UpsertParallel", func(b *testing.B) {
		var tr Tree
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			rng := pcg.New(pcg.Uint64())
			for pb.Next() {
				tr.Upsert(ikey(int(rng.Uint32n(10000))), nil)
			}
		})
	})

	b.Run("Iterate", func(b *testing.B) {
		var tr Tree
		for i := 0; i < 10000; i++ {
			tr.Upsert(ikey(i), nil)
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
