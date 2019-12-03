package lsm

import "testing"

type testMem interface {
	SetBytes([]byte, []byte) bool
	init(cap uint64)
	reset()

	iterGen() interface{ Next() bool }
}

func benchmarkMem(b *testing.B, mm func(cap uint64) testMem) {
	b.Run("Insert", func(b *testing.B) {
		m := mm(16 << 20)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if !m.SetBytes(getKey(i%numKeys), nil) {
				m.reset()
			}
		}
	})

	b.Run("Insert All", func(b *testing.B) {
		m := mm(16 << 20)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; ; j++ {
				if !m.SetBytes(getKey(j%numKeys), nil) {
					break
				}
			}
			m.reset()
		}
	})

	b.Run("Insert Iter", func(b *testing.B) {
		m := mm(16 << 20)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; ; j++ {
				if !m.SetBytes(getKey(j%numKeys), nil) {
					it := m.iterGen()
					for it.Next() {
					}
					break
				}
			}
			m.reset()
		}
	})
}
