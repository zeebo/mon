package lsm

import (
	"io"
	"testing"

	"github.com/zeebo/assert"
)

func TestFileIterator(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		entries, cleanup := tempWriteHandle(t, 4096)
		defer cleanup()
		values, cleanup := tempWriteHandle(t, 4096)
		defer cleanup()

		assert.NoError(t, writeFile(newFakeIterRandom(4096), entries, values))

		writeHandleSeekStart(t, entries)
		writeHandleSeekStart(t, values)

		fi := newFileIterator(entries.fh, values.fh)
		for prev := ""; fi.Next(); {
			key := string(fi.Key())
			assert.That(t, prev < key)
			assert.NotNil(t, fi.Value())
			prev = key
		}
		assert.NoError(t, fi.Err())
	})
}

func BenchmarkFileIterator(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		entries, cleanup := tempWriteHandle(b, 4096)
		defer cleanup()
		values, cleanup := tempWriteHandle(b, 4096)
		defer cleanup()

		assert.NoError(b, writeFile(newFakeIterRandom(4096), entries, values))

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			writeHandleSeekStart(b, entries)
			writeHandleSeekStart(b, values)
			fi := newFileIterator(entries.fh, values.fh)
			b.StartTimer()

			for fi.Next() {
				_ = fi.Key()
				_ = fi.Value()
			}
		}

		entriesSize, _ := entries.fh.Seek(0, io.SeekEnd)
		valuesSize, _ := values.fh.Seek(0, io.SeekEnd)
		b.SetBytes(entriesSize + valuesSize)
	})
}
