package lsm

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestMergedIterator(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var (
			keys   = "059acdeqrstuz"
			values = "DDDAABADBBEBD"
		)

		mi := newMerger([]iterator{
			newFakeIter("A", "a", "c", "e"),
			newFakeIter("B", "d", "r", "s", "u"),
			newFakeIter("C"),
			newFakeIter("D", "0", "5", "9", "q", "z"),
			newFakeIter("E", "5", "a", "t", "u"),
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
