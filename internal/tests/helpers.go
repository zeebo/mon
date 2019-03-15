package tests

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"unsafe"

	"github.com/zeebo/pcg"
)

var Value = unsafe.Pointer(new(int))

func Empty() unsafe.Pointer { return Value }

const (
	Size = 1 << 14
	Mask = Size - 1
)

var (
	ptrs = make([]unsafe.Pointer, Size)
	keys = make([]string, Size)
)

func init() {
	for i := range ptrs {
		ptrs[i&Mask] = unsafe.Pointer(new(int))
		keys[i&Mask] = fmt.Sprintf("%064d", i)
	}
}

func Ptr(i uint32) (p unsafe.Pointer) { return ptrs[i&Mask] }

func Key(i uint32) (s string) { return keys[i&Mask] }

type Type interface {
	Upsert(string, func() unsafe.Pointer) unsafe.Pointer
	Lookup(string) unsafe.Pointer
}

func RunBenchmarks(b *testing.B, fn func() Type) {
	rng := pcg.New(0)

	b.Run("UpsertFull", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			t := fn()
			for i := 0; i < Size; i++ {
				t.Upsert(Key(rng.Uint32n(Size)), Empty)
			}
		}
	})

	b.Run("Upsert", func(b *testing.B) {
		t := fn()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			t.Upsert(Key(rng.Uint32n(Size)), Empty)
		}
	})

	b.Run("Lookup", func(b *testing.B) {
		var sink unsafe.Pointer
		t := fn()
		for i := uint32(0); i < Size; i++ {
			t.Upsert(Key(i), Empty)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			sink = t.Lookup(Key(rng.Uint32n(Size)))
		}

		runtime.KeepAlive(sink)
	})

	b.Run("UpsertParallel", func(b *testing.B) {
		t := fn()
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			rng := pcg.New(pcg.Uint64())
			for pb.Next() {
				t.Upsert(Key(rng.Uint32n(Size)), Empty)
			}
		})
	})

	b.Run("UpsertFullParallel", func(b *testing.B) {
		procs := runtime.GOMAXPROCS(-1)
		iters := Size / procs
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			t := fn()
			var wg sync.WaitGroup

			for i := 0; i < procs; i++ {
				wg.Add(1)
				go func() {
					rng := pcg.New(pcg.Uint64())
					for i := 0; i < iters; i++ {
						t.Upsert(Key(rng.Uint32n(Size)), Empty)
					}
					wg.Done()
				}()
			}
			wg.Wait()
		}
	})
}
