package inthist

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/buffer"
	"github.com/zeebo/pcg"
)

func TestVarint(t *testing.T) {
	var le = binary.LittleEndian

	t.Run("Safe", func(t *testing.T) {
		for i := uint(0); i <= 32; i++ {
			buf := buffer.Of(make([]byte, 5))

			nbytes, val := varintStats(1<<i - 1)
			buf = buf.Grow()
			le.PutUint64(buf.Front8()[:], val)
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
			le.PutUint64(buf.Front8()[:], val)
			buf = buf.Advance(uintptr(nbytes))

			_, dec := fastVarintConsume(le.Uint64(buf.Reset().Front8()[:]))
			assert.Equal(t, uint32(1<<i-1), dec)

			t.Logf("%-2d %032b %08b\n", i, dec, buf.Prefix())
		}
	})
}

func BenchmarkVarint(b *testing.B) {
	var le = binary.LittleEndian

	randVals := make([]uint32, 1024*1024)
	for i := range randVals {
		randVals[i] = uint32(1<<pcg.Uint32n(32) - 1)
	}
	randBuf := buffer.Of(make([]byte, 16))
	for _, val := range randVals {
		nbytes, enc := varintStats(val)
		randBuf = randBuf.Grow()
		le.PutUint64(randBuf.Front8()[:], enc)
		randBuf = randBuf.Advance(uintptr(nbytes))
	}
	randBuf = randBuf.Reset()

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

		b.Run("Rand", func(b *testing.B) {
			var x uint8
			var y uint64
			for i := 0; i < b.N; i++ {
				x, y = varintStats(randVals[i%(1024*1024)])
			}
			runtime.KeepAlive(x)
			runtime.KeepAlive(y)
		})
	})

	b.Run("Append", func(b *testing.B) {
		for _, i := range []uint{1, 32} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint32(1<<i - 1)
				buf := buffer.Of(make([]byte, 16))

				for i := 0; i < b.N; i++ {
					_, val := varintStats(n)
					buf = buf.Grow()
					le.PutUint64(buf.Front8()[:], val)
				}
			})
		}

		b.Run("Rand", func(b *testing.B) {
			buf := buffer.Of(make([]byte, 16))

			for i := 0; i < b.N; i++ {
				_, val := varintStats(randVals[i%(1024*1024)])
				buf = buf.Grow()
				le.PutUint64(buf.Front8()[:], val)
			}
		})
	})

	b.Run("Consume", func(b *testing.B) {
		for _, i := range []uint{1, 32} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint32(1<<i - 1)
				buf := buffer.Of(make([]byte, 5))

				nbytes, val := varintStats(n)
				buf = buf.Grow()
				le.PutUint64(buf.Front8()[:], val)
				buf = buf.Advance(uintptr(nbytes))

				for i := 0; i < b.N; i++ {
					safeVarintConsume(buf)
				}
			})
		}

		b.Run("Rand", func(b *testing.B) {
			buf := randBuf.Reset()
			for i := 0; i < b.N; i++ {
				if buf.Remaining() == 0 {
					buf = buf.Reset()
				}
				_, buf, _ = safeVarintConsume(buf)
			}
		})
	})

	b.Run("FastConsume", func(b *testing.B) {
		for _, i := range []uint{1, 32} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint32(1<<i - 1)
				buf := buffer.Of(make([]byte, 8))

				nbytes, val := varintStats(n)
				buf = buf.Grow()
				le.PutUint64(buf.Front8()[:], val)
				buf = buf.Advance(uintptr(nbytes))

				var dec uint32
				for i := 0; i < b.N; i++ {
					_, dec = fastVarintConsume(le.Uint64(buf.Front8()[:]))
				}
				runtime.KeepAlive(dec)
			})
		}

		b.Run("Rand", func(b *testing.B) {
			buf := randBuf.Reset()
			for i := 0; i < b.N; i++ {
				if buf.Remaining() < 8 {
					buf = buf.Reset()
				}
				nbytes, _ := fastVarintConsume(le.Uint64(buf.Front8()[:]))
				buf = buf.Advance(uintptr(nbytes))
			}
		})
	})
}
