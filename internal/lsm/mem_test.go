package lsm

import (
	"fmt"
	"testing"

	"github.com/zeebo/pcg"
)

func BenchmarkMem(b *testing.B) {
	b.Run("Iterator", func(b *testing.B) {
		var rng pcg.T

		m := newMem(1 << 20)
		for m.SetString(fmt.Sprint(rng.Uint64()), []byte(fmt.Sprint(rng.Uint64()))) {
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Iterator()
		}
	})
}
