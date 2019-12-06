package mergeiter

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/lsm/iterator"
	"github.com/zeebo/mon/internal/lsm/testutil"
)

func TestMergedIterator(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var (
			keys   = "059acdeqrstuz"
			values = "DDDAABADBBEBD"
		)

		mi := New([]iterator.T{
			testutil.NewFakeIterator("A", "a", "c", "e"),
			testutil.NewFakeIterator("B", "d", "r", "s", "u"),
			testutil.NewFakeIterator("C"),
			testutil.NewFakeIterator("D", "0", "5", "9", "q", "z"),
			testutil.NewFakeIterator("E", "5", "a", "t", "u"),
		})

		for mi.Next() {
			assert.Equal(t, string(mi.Key()), keys[0:1])
			assert.Equal(t, string(mi.Value()), values[0:1])
			keys, values = keys[1:], values[1:]
		}
		assert.NoError(t, mi.Err())
		assert.Equal(t, keys, "")
		assert.Equal(t, values, "")
	})
}
