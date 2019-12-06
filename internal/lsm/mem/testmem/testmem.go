package testmem

import (
	"testing"

	"github.com/zeebo/mon/internal/lsm/iterator"
	"github.com/zeebo/mon/internal/lsm/testutil"
)

type T interface {
	SetBytes([]byte, []byte) bool
	Init(cap uint64)
	Reset()
	Iters() []iterator.T
}

func Benchmark(b *testing.B, mm func(cap uint64) T) {
	b.Run("Insert", func(b *testing.B) {
		m := mm(16 << 20)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if !m.SetBytes(testutil.GetKey(i), nil) {
				m.Reset()
			}
		}
	})

	b.Run("Insert All", func(b *testing.B) {
		m := mm(16 << 20)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; ; j++ {
				if !m.SetBytes(testutil.GetKey(j), nil) {
					break
				}
			}
			m.Reset()
		}
	})

	b.Run("Insert Iter", func(b *testing.B) {
		m := mm(16 << 20)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; ; j++ {
				if !m.SetBytes(testutil.GetKey(j), nil) {
					it := m.Iters()[0]
					for it.Next() {
					}
					break
				}
			}
			m.Reset()
		}
	})
}
