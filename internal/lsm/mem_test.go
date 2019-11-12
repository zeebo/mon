package lsm

import (
	"fmt"
	"testing"

	"github.com/zeebo/pcg"
)

func BenchmarkMem(b *testing.B) {
	b.Run("AppendTo", func(b *testing.B) {
		var rng pcg.T

		m := newMem(1024)
		for i := 0; i < 1000; i++ {
			m.SetString(fmt.Sprint(rng.Uint64()), []byte(fmt.Sprint(rng.Uint64())))
		}
		out := m.AppendTo(nil)

		b.SetBytes(int64(len(out)))
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			out = m.AppendTo(out[:0])
		}
	})
}
