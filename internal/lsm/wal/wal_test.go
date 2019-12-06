package wal

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/testutil"
)

func BenchmarkWAL(b *testing.B) {
	b.Run("AddString", func(b *testing.B) {
		fh, cleanup := testutil.TempFile(b)
		defer cleanup()

		w := New(fh, false)
		value := make([]byte, 128)
		b.SetBytes(int64(entry.Size + len(value)))
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			assert.NoError(b, w.AddString("hello", value))
		}
	})
}
