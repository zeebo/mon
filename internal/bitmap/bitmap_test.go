package bitmap

import (
	"math"
	"runtime"
	"testing"
)

func TestBitmap64(t *testing.T) {
	var b B64

	for i := uint(0); i < 64; i++ {
		b.Set(i)

		got, ok := b.Next()
		if !ok || got != i {
			t.Fatal(i)
		}
		if b != (B64{}) {
			t.Fatal(b)
		}
	}
}

func TestBitmap128(t *testing.T) {
	var b B128

	for i := uint(0); i < 128; i++ {
		b.Set(i)

		got, ok := b.Next()
		if !ok || got != i {
			t.Fatal(i)
		}
		if b != (B128{}) {
			t.Fatal(b)
		}
	}
}

func BenchmarkBitmap64(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		idx := uint(0)
		for i := 0; i < b.N; i++ {
			bm := B64{1}
			idx, _ = bm.Next()
		}
		runtime.KeepAlive(idx)
	})

	b.Run("NextAll", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b := B64{math.MaxUint64}
			for {
				_, ok := b.Next()
				if !ok {
					break
				}
			}
		}
	})
}

func BenchmarkBitmap128(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		idx := uint(0)
		for i := 0; i < b.N; i++ {
			bm := B128{1, 0}
			idx, _ = bm.Next()
		}
		runtime.KeepAlive(idx)
	})

	b.Run("NextAll", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b := B128{math.MaxUint64, math.MaxUint64}
			for {
				_, ok := b.Next()
				if !ok {
					break
				}
			}
		}
	})
}
