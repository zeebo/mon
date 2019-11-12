package lsm

import (
	"bytes"
	"io"
	"testing"

	"github.com/zeebo/assert"
)

func TestWALIter(t *testing.T) {
	var buf bytes.Buffer
	w := NewWALUnsafe(&buf, 1024)
	_, _ = w.AddString("0", []byte("0"))
	_, _ = w.DelString("1")
	_, _ = w.AddString("2", []byte("2"))

	wi := NewWALIterator(bytes.NewReader(buf.Bytes()))

	ent, key, value, err := wi.Next()
	consumed, prefix := wi.Consumed()
	assert.Equal(t, ent, newEntry(newInlinePtrString("0"), newInlinePtrString("0")))
	assert.Equal(t, string(key), "0")
	assert.Equal(t, string(value), "0")
	assert.NoError(t, err)
	assert.Equal(t, consumed, 34)
	assert.That(t, !prefix)

	ent, key, value, err = wi.Next()
	consumed, prefix = wi.Consumed()
	assert.Equal(t, ent, newEntry(newInlinePtrString("1"), inlinePtr{}))
	assert.Equal(t, string(key), "1")
	assert.Nil(t, value)
	assert.NoError(t, err)
	assert.Equal(t, consumed, 67)
	assert.That(t, !prefix)

	ent, key, value, err = wi.Next()
	consumed, prefix = wi.Consumed()
	assert.Equal(t, ent, newEntry(newInlinePtrString("2"), newInlinePtrString("2")))
	assert.Equal(t, string(key), "2")
	assert.Equal(t, string(value), "2")
	assert.NoError(t, err)
	assert.Equal(t, consumed, 101)
	assert.That(t, !prefix)

	ent, key, value, err = wi.Next()
	consumed, prefix = wi.Consumed()
	assert.Equal(t, ent, entry{})
	assert.Nil(t, key)
	assert.Nil(t, value)
	assert.Equal(t, err, io.EOF)
	assert.Equal(t, consumed, 101)
	assert.That(t, !prefix)
}

func TestWALIter_Truncated(t *testing.T) {
	var buf bytes.Buffer
	w := NewWALUnsafe(&buf, 1024)
	_, _ = w.AddString("0", []byte("0"))
	_, _ = w.AddString("01235", []byte("01235"))

	wi := NewWALIterator(bytes.NewReader(buf.Bytes()[:68]))

	ent, key, value, err := wi.Next()
	consumed, prefix := wi.Consumed()
	assert.Equal(t, ent, newEntry(newInlinePtrString("0"), newInlinePtrString("0")))
	assert.Equal(t, string(key), "0")
	assert.Equal(t, string(value), "0")
	assert.NoError(t, err)
	assert.Equal(t, consumed, 34)
	assert.That(t, !prefix)

	ent, key, value, err = wi.Next()
	consumed, prefix = wi.Consumed()
	assert.Equal(t, ent, entry{})
	assert.Nil(t, key)
	assert.Nil(t, value)
	assert.Equal(t, err, io.ErrUnexpectedEOF)
	assert.Equal(t, consumed, 34)
	assert.That(t, prefix)
}
