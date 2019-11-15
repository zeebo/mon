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
	var rng pcg.T
	m := newMem(1 << 20)
	for m.SetString(fmt.Sprint(rng.Uint32()), make([]byte, 128)) {
	}

	mg, err := newMerger([]mergeIter{m})
	assert.NoError(t, err)

	entries, cleanup := tempHandle(t, 4096)
	defer cleanup()
	values, cleanup := tempHandle(t, 4096)
	defer cleanup()

	assert.NoError(t, writeFile(mg, entries, values))

	handleSeekStart(t, entries)
	handleSeekStart(t, values)

	fi := newFileIterator(entries, values)
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

		data, err := bi.ReadPointer(*ent.Value())
		assert.NoError(t, err)
		assert.That(t, bytes.Equal(data, make([]byte, 128)))
	}
}

func BenchmarkFileIterator(b *testing.B) {
	var rng pcg.T
	m := newMem(1 << 20)
	for m.SetString(fmt.Sprint(rng.Uint32()), make([]byte, 128)) {
	}

	mg, err := newMerger([]mergeIter{m})
	assert.NoError(b, err)

	entries, cleanup := tempHandle(b, 4096)
	defer cleanup()
	values, cleanup := tempHandle(b, 4096)
	defer cleanup()

	assert.NoError(b, writeFile(mg, entries, values))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		handleSeekStart(b, entries)
		handleSeekStart(b, values)
		fi := newFileIterator(entries, values)
		bi := newBatchMergeIterAdapter(fi, 4096/32)
		b.StartTimer()

		for {
			ent, err := bi.Next()
			if err == io.EOF {
				break
			}

			if kptr := ent.Key(); kptr.Pointer() {
				_, _ = bi.ReadPointer(*kptr)
			}
			if vptr := ent.Value(); vptr.Pointer() {
				_, _ = bi.ReadPointer(*vptr)
			}
		}
	}

	entriesSize, _ := entries.fh.Seek(0, io.SeekEnd)
	valuesSize, _ := values.fh.Seek(0, io.SeekEnd)
	b.SetBytes(entriesSize + valuesSize)
}
