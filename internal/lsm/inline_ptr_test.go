package lsm

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestInlinePtr(t *testing.T) {
	t.Run("SetOffset", func(t *testing.T) {
		var ptr inlinePtr
		assert.Equal(t, ptr.Offset(), 0)

		for i := 0; i < 40; i++ {
			ptr.SetOffset(1 << i)
			assert.Equal(t, ptr.Offset(), 1<<i)
		}
	})
}
