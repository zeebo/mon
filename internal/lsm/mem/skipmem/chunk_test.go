package skipmem

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestChunk(t *testing.T) {
	var root chunk
	cur := root.cursor()

	for i := 0; i < 50; i++ {
		cur.insert(chunkEntry{0, uint32(i)})
		cur.right()
	}

	cur = root.cursor()
	for i := 0; i < 50; i++ {
		val := cur.get()
		assert.Equal(t, val, chunkEntry{0, uint32(i)})

		ok := cur.right()
		assert.That(t, ok != (i == 49))
	}

	cur.insert(chunkEntry{0, 99})
}
