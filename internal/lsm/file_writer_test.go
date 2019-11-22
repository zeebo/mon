package lsm

import (
	"io"
	"testing"

	"github.com/zeebo/assert"
)

func TestFileWriter(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		entries, cleanup := tempWriteHandle(t, 4096)
		defer cleanup()
		values, cleanup := tempWriteHandle(t, 4096)
		defer cleanup()

		assert.NoError(t, writeFile(newFakeIterRandom(4096), entries, values))
	})
}

func BenchmarkFileWriter(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		entries, cleanup := tempWriteHandle(b, 4096)
		defer cleanup()
		values, cleanup := tempWriteHandle(b, 4096)
		defer cleanup()

		mi := newFakeIterRandom(4096)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			writeHandleReset(b, entries)
			writeHandleReset(b, values)
			mi := *mi
			b.StartTimer()

			assert.NoError(b, writeFile(&mi, entries, values))
		}

		entriesSize, _ := entries.fh.Seek(0, io.SeekEnd)
		valuesSize, _ := values.fh.Seek(0, io.SeekEnd)
		b.SetBytes(entriesSize + valuesSize)
	})
}
