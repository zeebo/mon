package layermem

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func contains(x []uint8, v uint8) bool {
	for i := range x {
		if x[i] == v {
			return true
		}
	}
	return false
}

func TestSort(t *testing.T) {
	var rng pcg.T

	for i := 0; i < 1000; i++ {
		in := [...]layerEntry{
			{rng.Uint64(), 0},
			{rng.Uint64(), 0},
			{rng.Uint64(), 0},
			{rng.Uint64(), 0},
		}

		idxs := sortEntries4(&in)
		// t.Logf("%016x %d", in, idxs)

		for i := 0; i < len(idxs); i++ {
			assert.That(t, contains(idxs[:], uint8(i)))
		}
		for i, v := range idxs[1:] {
			assert.That(t, in[idxs[i]].prefix < in[v].prefix)
		}
	}
}

func BenchmarkSort(b *testing.B) {
	x := &[...]layerEntry{{5, 0}, {7, 0}, {0, 0}, {2, 0}}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sortEntries4(x)
	}
}
