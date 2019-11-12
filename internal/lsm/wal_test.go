package lsm

import (
	"io/ioutil"
	"testing"
)

func BenchmarkWAL(b *testing.B) {
	b.Run("AddString", func(b *testing.B) {
		w := NewWALUnsafe(ioutil.Discard, 1024)
		b.SetBytes(int64(entrySize + 5))
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, _ = w.AddString("hello", []byte("there"))
		}
	})
}
