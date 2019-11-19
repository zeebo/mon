package lsm

import (
	"bytes"
	"io"
	"testing"

	"github.com/zeebo/assert"
)

func TestWALIter(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		fh, cleanup := tempFile(t)
		defer cleanup()

		w := newWAL(fh, false)
		assert.NoError(t, w.AddString("0", []byte("0")))
		assert.NoError(t, w.AddString("1", nil))
		assert.NoError(t, w.AddString("2", []byte("2")))
		assert.NoError(t, w.Flush())

		fileSeekStart(t, fh)
		wi := newWALIterator(fh)

		ent, key, value, err := wi.Next()
		consumed, prefix := wi.Consumed()
		assert.Equal(t, ent, newEntry(newInlinePtrString("0"), newInlinePtrString("0")))
		assert.Equal(t, string(key), "0")
		assert.Equal(t, string(value), "0")
		assert.NoError(t, err)
		assert.Equal(t, consumed, 32)
		assert.That(t, !prefix)

		ent, key, value, err = wi.Next()
		consumed, prefix = wi.Consumed()
		assert.Equal(t, ent, newEntry(newInlinePtrString("1"), inlinePtr{}))
		assert.Equal(t, string(key), "1")
		assert.Nil(t, value)
		assert.NoError(t, err)
		assert.Equal(t, consumed, 64)
		assert.That(t, !prefix)

		ent, key, value, err = wi.Next()
		consumed, prefix = wi.Consumed()
		assert.Equal(t, ent, newEntry(newInlinePtrString("2"), newInlinePtrString("2")))
		assert.Equal(t, string(key), "2")
		assert.Equal(t, string(value), "2")
		assert.NoError(t, err)
		assert.Equal(t, consumed, 96)
		assert.That(t, !prefix)

		ent, key, value, err = wi.Next()
		consumed, prefix = wi.Consumed()
		assert.Equal(t, ent, entry{})
		assert.Nil(t, key)
		assert.Nil(t, value)
		assert.Equal(t, err, io.EOF)
		assert.Equal(t, consumed, 96)
		assert.That(t, !prefix)
	})

	t.Run("Truncated", func(t *testing.T) {
		fh, cleanup := tempFile(t)
		defer cleanup()

		w := newWAL(fh, false)
		assert.NoError(t, w.AddString("0", []byte("0")))
		assert.NoError(t, w.AddString("01235", []byte("01235")))
		assert.NoError(t, w.Flush())

		wi := newWALIterator(bytes.NewReader(fileContents(t, fh)[:62]))

		ent, key, value, err := wi.Next()
		consumed, prefix := wi.Consumed()
		assert.Equal(t, ent, newEntry(newInlinePtrString("0"), newInlinePtrString("0")))
		assert.Equal(t, string(key), "0")
		assert.Equal(t, string(value), "0")
		assert.NoError(t, err)
		assert.Equal(t, consumed, 32)
		assert.That(t, !prefix)

		ent, key, value, err = wi.Next()
		consumed, prefix = wi.Consumed()
		assert.Equal(t, ent, entry{})
		assert.Nil(t, key)
		assert.Nil(t, value)
		assert.Equal(t, err, io.ErrUnexpectedEOF)
		assert.Equal(t, consumed, 32)
		assert.That(t, prefix)
	})
}
