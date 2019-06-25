package mon

import (
	"fmt"
	"testing"
)

func TestVarint(t *testing.T) {
	for i := uint(0); i <= 32; i++ {
		buf := bufferOf(make([]byte, 5))
		buf = varintAppend(buf, 1<<i-1)
		// buf.cap = 5
		got, _ := varintConsume(buf.reset())

		t.Logf("%-2d %032b %08b\n", i, got, buf.prefix())
	}
}

func BenchmarkVarint(b *testing.B) {
	b.Run("Append", func(b *testing.B) {
		for _, i := range []uint{1, 32} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint32(1<<i - 1)
				buf := bufferOf(make([]byte, 16))

				for i := 0; i < b.N; i++ {
					varintAppend(buf, n)
				}
			})
		}
	})

	b.Run("Consume", func(b *testing.B) {
		for _, i := range []uint{1, 32} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint32(1<<i - 1)
				buf := bufferOf(make([]byte, 5))
				buf = varintAppend(buf, n).reset()
				// buf.cap = 5

				for i := 0; i < b.N; i++ {
					varintConsume(buf)
				}
			})
		}
	})
}
