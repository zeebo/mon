package skipmem

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestChunkList(t *testing.T) {
	cl := new(chunkList)
	cur := cl.cursor()

	for i := 0; i < 50; i++ {
		cur = cur.insert(skipMemEntry{val: uint32(i)})
	}

	cur = cl.cursor()
	for i := 0; i < 51; i++ {
		val, ok := cur.get()
		assert.That(t, ok != (i == 50))
		if ok {
			assert.Equal(t, val.val, uint32(i))
		}

		ncur, ok := cur.next()
		assert.That(t, ok != (i == 50))
		if ok {
			cur = ncur
		}
	}

	cur.insert(skipMemEntry{val: 99})
}
