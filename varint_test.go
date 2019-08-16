package mon

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/buffer"
)

func TestVarint(t *testing.T) {
	var le = binary.LittleEndian

	for i := uint(0); i <= 32; i++ {
		buf := buffer.Of(make([]byte, 5))

		nbytes, val := varintStats(1<<i - 1)
		buf = buf.Grow()
		le.PutUint64(buf.Front()[:], val)
		buf = buf.Advance(uintptr(nbytes))

		got, _ := varintConsume(buf.Reset())
		assert.Equal(t, uint32(1<<i-1), got)

		t.Logf("%-2d %032b %08b\n", i, got, buf.Prefix())
	}
}

func BenchmarkVarint(b *testing.B) {
	var le = binary.LittleEndian

	b.Run("Append", func(b *testing.B) {
		for _, i := range []uint{1, 32} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint32(1<<i - 1)
				buf := buffer.Of(make([]byte, 16))

				for i := 0; i < b.N; i++ {
					_, val := varintStats(n)
					buf = buf.Grow()
					le.PutUint64(buf.Front()[:], val)
				}
			})
		}
	})

	b.Run("Consume", func(b *testing.B) {
		for _, i := range []uint{1, 32} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint32(1<<i - 1)
				buf := buffer.Of(make([]byte, 5))

				nbytes, val := varintStats(n)
				buf = buf.Grow()
				le.PutUint64(buf.Front()[:], val)
				buf = buf.Advance(uintptr(nbytes))

				for i := 0; i < b.N; i++ {
					varintConsume(buf)
				}
			})
		}
	})
}
