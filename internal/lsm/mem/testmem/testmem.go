package testmem

import (
	"testing"

	"github.com/zeebo/mon/internal/lsm/iterator"
	"github.com/zeebo/mon/internal/lsm/testutil"
)

const MemCap = 16 << 20

var value = make([]byte, testutil.ValueLength)

type T interface {
	SetBytes([]byte, []byte) bool
	Init(cap uint64)
	Reset()
	Iters() []iterator.T

	Len() uint64
	Cap() uint64
}

func Benchmark(b *testing.B, mm func(cap uint64) T) {
	b.Run("Insert", func(b *testing.B) {
		m := mm(MemCap)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if !m.SetBytes(testutil.GetKey(i), value) {
				m.Reset()
			}
		}
	})

	b.Run("Insert All", func(b *testing.B) {
		m := mm(MemCap)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; ; j++ {
				if !m.SetBytes(testutil.GetKey(j), value) {
					break
				}
			}
			m.Reset()
		}
	})

	b.Run("Insert Iter", func(b *testing.B) {
		m := mm(MemCap)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; ; j++ {
				if !m.SetBytes(testutil.GetKey(j), value) {
					it := m.Iters()[0]
					for it.Next() {
					}
					if i == 0 {
						b.Log(j)
					}
					break
				}
			}
			m.Reset()
		}
	})
}
