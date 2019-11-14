package lsm

import (
	"testing"
)

func BenchmarkWAL(b *testing.B) {
	b.Run("AddString", func(b *testing.B) {
		w := newWAL(nullFile, 1024)
		b.SetBytes(int64(entrySize + 5))
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, _ = w.AddString("hello", []byte("there"))
		}
	})
}
