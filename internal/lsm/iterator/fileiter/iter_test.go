package fileiter

import (
	"io"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/lsm/file/filewriter"
	"github.com/zeebo/mon/internal/lsm/testutil"
)

func TestFileIterator(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		entries, cleanup := testutil.TempWriteHandle(t, 4096)
		defer cleanup()
		values, cleanup := testutil.TempWriteHandle(t, 4096)
		defer cleanup()

		assert.NoError(t, filewriter.Write(testutil.NewRandomFakeIterator(4096), entries, values))

		testutil.SeekStartWriteHandle(t, entries)
		testutil.SeekStartWriteHandle(t, values)

		fi := New(entries.File(), values.File())
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
		entries, cleanup := testutil.TempWriteHandle(b, 4096)
		defer cleanup()
		values, cleanup := testutil.TempWriteHandle(b, 4096)
		defer cleanup()
		var fi T

		assert.NoError(b, filewriter.Write(testutil.NewRandomFakeIterator(4096), entries, values))

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			testutil.SeekStartWriteHandle(b, entries)
			testutil.SeekStartWriteHandle(b, values)
			fi.Init(entries.File(), values.File())
			b.StartTimer()

			for fi.Next() {
				_ = fi.Key()
				_ = fi.Value()
			}
		}

		entriesSize, _ := entries.File().Seek(0, io.SeekEnd)
		valuesSize, _ := values.File().Seek(0, io.SeekEnd)
		b.SetBytes(entriesSize + valuesSize)
	})
}
