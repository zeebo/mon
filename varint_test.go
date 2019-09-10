package mon

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/buffer"
)

func TestVarint(t *testing.T) {
	var le = binary.LittleEndian

	t.Run("Safe", func(t *testing.T) {
		for i := uint(0); i <= 32; i++ {
			buf := buffer.Of(make([]byte, 5))

			nbytes, val := varintStats(1<<i - 1)
			buf = buf.Grow()
			le.PutUint64(buf.Front()[:], val)
			buf = buf.Advance(uintptr(nbytes))

			got, _, ok := safeVarintConsume(buf.Reset())
			assert.That(t, ok)
			assert.Equal(t, uint32(1<<i-1), got)

			t.Logf("%-2d %032b %08b\n", i, got, buf.Prefix())
		}
	})

	t.Run("Fast", func(t *testing.T) {
		for i := uint(0); i <= 32; i++ {
			buf := buffer.Of(make([]byte, 8))

			nbytes, val := varintStats(1<<i - 1)
			buf = buf.Grow()
			le.PutUint64(buf.Front()[:], val)
			buf = buf.Advance(uintptr(nbytes))

			_, dec := fastVarintConsume(le.Uint64(buf.Reset().Front()[:]))
			assert.Equal(t, uint32(1<<i-1), dec)

			t.Logf("%-2d %032b %08b\n", i, dec, buf.Prefix())
		}
	})
}

func BenchmarkVarint(b *testing.B) {
	var le = binary.LittleEndian

	b.Run("Stats", func(b *testing.B) {
		for _, i := range []uint{1, 32} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint32(1<<i - 1)

				var x uint8
				var y uint64
				for i := 0; i < b.N; i++ {
					x, y = varintStats(n)
				}
				runtime.KeepAlive(x)
				runtime.KeepAlive(y)
			})
		}
	})

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
					safeVarintConsume(buf)
				}
			})
		}
	})

	b.Run("FastConsume", func(b *testing.B) {
		for _, i := range []uint{1, 32} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint32(1<<i - 1)
				buf := buffer.Of(make([]byte, 8))

				nbytes, val := varintStats(n)
				buf = buf.Grow()
				le.PutUint64(buf.Front()[:], val)
				buf = buf.Advance(uintptr(nbytes))

				var dec uint32
				for i := 0; i < b.N; i++ {
					_, dec = fastVarintConsume(le.Uint64(buf.Front()[:]))
				}
				runtime.KeepAlive(dec)
			})
		}
	})
}
