package lsm

import (
	"testing"

	"github.com/zeebo/assert"
)

func BenchmarkWAL(b *testing.B) {
	b.Run("AddString", func(b *testing.B) {
		fh, cleanup := tempFile(b)
		defer cleanup()

		w := newWAL(fh, false)
		value := make([]byte, 128)
		b.SetBytes(int64(entrySize + len(value)))
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			assert.NoError(b, w.AddString("hello", value))
		}
	})
}
