package smallsort

import (
	"sort"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestSort(t *testing.T) {
	var rng pcg.T

	for i := 0; i < 1000; i++ {
		in := [...]uint64{
			rng.Uint64() >> 8, rng.Uint64() >> 8, rng.Uint64() >> 8, rng.Uint64() >> 8,
			rng.Uint64() >> 8, rng.Uint64() >> 8, rng.Uint64() >> 8, rng.Uint64() >> 8,
		}

		idxs := Sort(in)
		for i, v := range idxs[1:] {
			assert.That(t, in[idxs[i]] <= in[v])
		}
	}
}

func BenchmarkSort(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		Sort([...]uint64{5, 7, 0, 2, 6, 1, 3, 4})
	}
}

func BenchmarkStdlib(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sort.Sort(uint64Slice{5, 7, 0, 2, 6, 1, 3, 4})
	}
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
