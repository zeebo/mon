package waliter

import (
	"io"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
	"github.com/zeebo/mon/internal/lsm/testutil"
	"github.com/zeebo/mon/internal/lsm/wal"
)

func TestWALIter(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		fh, cleanup := testutil.TempFile(t)
		defer cleanup()

		w := wal.New(fh, false)
		assert.NoError(t, w.AddString("0", []byte("0")))
		assert.NoError(t, w.AddString("1", nil))
		assert.NoError(t, w.AddString("2", []byte("2")))
		assert.NoError(t, w.Flush())

		testutil.SeekStartFile(t, fh)
		wi := New(fh)

		ent, key, value, ok := wi.Next()
		consumed, prefix := wi.Consumed()
		assert.Equal(t, ent, entry.New(inlineptr.String("0"), inlineptr.String("0")))
		assert.Equal(t, string(key), "0")
		assert.Equal(t, string(value), "0")
		assert.That(t, ok)
		assert.Equal(t, consumed, 32)
		assert.That(t, !prefix)
		assert.NoError(t, wi.Err())

		ent, key, value, ok = wi.Next()
		consumed, prefix = wi.Consumed()
		assert.Equal(t, ent, entry.New(inlineptr.String("1"), inlineptr.T{}))
		assert.Equal(t, string(key), "1")
		assert.Nil(t, value)
		assert.That(t, ok)
		assert.Equal(t, consumed, 64)
		assert.That(t, !prefix)
		assert.NoError(t, wi.Err())

		ent, key, value, ok = wi.Next()
		consumed, prefix = wi.Consumed()
		assert.Equal(t, ent, entry.New(inlineptr.String("2"), inlineptr.String("2")))
		assert.Equal(t, string(key), "2")
		assert.Equal(t, string(value), "2")
		assert.That(t, ok)
		assert.Equal(t, consumed, 96)
		assert.That(t, !prefix)
		assert.NoError(t, wi.Err())

		ent, key, value, ok = wi.Next()
		consumed, prefix = wi.Consumed()
		assert.Equal(t, ent, entry.T{})
		assert.Nil(t, key)
		assert.Nil(t, value)
		assert.That(t, !ok)
		assert.Equal(t, consumed, 96)
		assert.That(t, !prefix)
		assert.NoError(t, wi.Err())
	})

	t.Run("Truncated", func(t *testing.T) {
		fh, cleanup := testutil.TempFile(t)
		defer cleanup()

		w := wal.New(fh, false)
		assert.NoError(t, w.AddString("0", []byte("0")))
		assert.NoError(t, w.AddString("01235", []byte("01235")))
		assert.NoError(t, w.Flush())

		assert.NoError(t, fh.Truncate(62))
		testutil.SeekStartFile(t, fh)
		wi := New(fh)

		ent, key, value, ok := wi.Next()
		consumed, prefix := wi.Consumed()
		assert.Equal(t, ent, entry.New(inlineptr.String("0"), inlineptr.String("0")))
		assert.Equal(t, string(key), "0")
		assert.Equal(t, string(value), "0")
		assert.That(t, ok)
		assert.Equal(t, consumed, 32)
		assert.That(t, !prefix)
		assert.NoError(t, wi.Err())

		ent, key, value, ok = wi.Next()
		consumed, prefix = wi.Consumed()
		assert.Equal(t, ent, entry.T{})
		assert.Nil(t, key)
		assert.Nil(t, value)
		assert.That(t, !ok)
		assert.Equal(t, consumed, 32)
		assert.That(t, prefix)
		assert.Equal(t, wi.Err(), io.EOF)
	})
}
