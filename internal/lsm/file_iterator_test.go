package lsm

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestFileIterator(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var rng pcg.T
		m := newMem(1 << 20)
		for m.SetString(fmt.Sprint(rng.Uint32()), make([]byte, 128)) {
		}

		mg, err := newMerger([]mergeIter{m})
		assert.NoError(t, err)

		entries, cleanup := tempWriteHandle(t, 4096)
		defer cleanup()
		values, cleanup := tempWriteHandle(t, 4096)
		defer cleanup()

		assert.NoError(t, writeFile(mg, entries, values))

		writeHandleSeekStart(t, entries)
		writeHandleSeekStart(t, values)

		fi, err := newFileIterator(entries.fh, values.fh)
		assert.NoError(t, err)
		bi := newBatchMergeIterAdapter(fi, 4096/32)
		prev := ""

		for {
			ent, err := bi.Next()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err)

			key := string(ent.Key().InlineData())
			assert.That(t, prev < key)
			prev = key

			data, err := bi.AppendPointer(*ent.Value(), nil)
			assert.NoError(t, err)
			assert.That(t, bytes.Equal(data, make([]byte, 128)))
		}
	})
}

func BenchmarkFileIterator(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		var rng pcg.T
		m := newMem(1 << 20)
		for m.SetString(fmt.Sprint(rng.Uint32()), make([]byte, 128)) {
		}

		mg, err := newMerger([]mergeIter{m})
		assert.NoError(b, err)

		entries, cleanup := tempWriteHandle(b, 4096)
		defer cleanup()
		values, cleanup := tempWriteHandle(b, 4096)
		defer cleanup()

		assert.NoError(b, writeFile(mg, entries, values))
		var buf []byte

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			writeHandleSeekStart(b, entries)
			writeHandleSeekStart(b, values)
			fi, err := newFileIterator(entries.fh, values.fh)
			assert.NoError(b, err)
			bi := newBatchMergeIterAdapter(fi, 4096/32)
			b.StartTimer()

			for {
				ent, err := bi.Next()
				if err == io.EOF {
					break
				}

				if kptr := ent.Key(); kptr.Pointer() {
					buf, _ = bi.AppendPointer(*kptr, buf[:0])
				}
				if vptr := ent.Value(); vptr.Pointer() {
					buf, _ = bi.AppendPointer(*vptr, buf[:0])
				}
			}
		}

		entriesSize, _ := entries.fh.Seek(0, io.SeekEnd)
		valuesSize, _ := values.fh.Seek(0, io.SeekEnd)
		b.SetBytes(entriesSize + valuesSize)
	})
}
