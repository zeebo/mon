package filewriter

import (
	"io"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/lsm/testutil"
)

func TestFileWriter(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		entries, cleanup := testutil.TempWriteHandle(t, 4096)
		defer cleanup()
		values, cleanup := testutil.TempWriteHandle(t, 4096)
		defer cleanup()

		assert.NoError(t, Write(testutil.NewRandomFakeIterator(4096), entries, values))
	})
}

func BenchmarkFileWriter(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		entries, cleanup := testutil.TempWriteHandle(b, 4096)
		defer cleanup()
		values, cleanup := testutil.TempWriteHandle(b, 4096)
		defer cleanup()

		mi := testutil.NewRandomFakeIterator(4096)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			testutil.ResetWriteHandle(b, entries)
			testutil.ResetWriteHandle(b, values)
			mi := *mi
			b.StartTimer()

			assert.NoError(b, Write(&mi, entries, values))
		}

		entriesSize, _ := entries.File().Seek(0, io.SeekEnd)
		valuesSize, _ := values.File().Seek(0, io.SeekEnd)
		b.SetBytes(entriesSize + valuesSize)
	})
}
