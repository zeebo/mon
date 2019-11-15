package lsm

import (
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestFileWriter(t *testing.T) {
	var rng pcg.T
	m := newMem(4096)
	for m.SetString(fmt.Sprint(rng.Uint32()), []byte(fmt.Sprint(rng.Uint32()))) {
	}

	mg, err := newMerger([]mergeIter{m})
	assert.NoError(t, err)

	entries, cleanup := tempHandle(t, 4096)
	defer cleanup()
	values, cleanup := tempHandle(t, 4096)
	defer cleanup()

	assert.NoError(t, writeFile(mg, entries, values))

	t.Log("\n" + hex.Dump(fileContents(t, entries.fh)))
	t.Log("\n" + hex.Dump(fileContents(t, values.fh)))
}

func BenchmarkFileWriter(b *testing.B) {
	var rng pcg.T
	m := newMem(1 << 20)
	for m.SetString(fmt.Sprint(rng.Uint32()), make([]byte, 128)) {
	}

	entries, cleanup := tempHandle(b, 4096)
	defer cleanup()
	values, cleanup := tempHandle(b, 4096)
	defer cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		handleReset(b, entries)
		handleReset(b, values)
		mg, err := newMerger([]mergeIter{m.iterClone()})
		assert.NoError(b, err)
		b.StartTimer()

		assert.NoError(b, writeFile(mg, entries, values))
	}

	entriesSize, _ := entries.fh.Seek(0, io.SeekEnd)
	valuesSize, _ := values.fh.Seek(0, io.SeekEnd)
	b.SetBytes(entriesSize + valuesSize)
}
