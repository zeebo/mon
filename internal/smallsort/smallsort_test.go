package smallsort

import (
	"sort"
	"testing"
	"unsafe"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

var values [1024 + 8]uint64

func init() {
	var rng pcg.T
	for i := range values {
		values[i] = rng.Uint64()
	}
}

func vals(i int) *[8]uint64 {
	return (*[8]uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(&values[0])) + 8*uintptr((i*1021)%1024)))
}

func contains(x [8]uint8, v uint8) bool {
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
		in := [...]uint64{
			rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64(),
			rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64(),
		}

		idxs := Sort(&in)
		t.Logf("%016x %d", in, idxs)

		for i := uint8(0); i < 8; i++ {
			assert.That(t, contains(idxs, i))
		}
		for i, v := range idxs[1:] {
			assert.That(t, in[idxs[i]] < in[v])
		}
	}
}

func TestMin(t *testing.T) {
	for a := 0; a < 8; a++ {
		for b := a; b < 8; b++ {
			in := [...]uint64{2, 2, 2, 2, 2, 2, 2, 2}
			in[a] = 0
			in[b] = 0

			t.Log(in)
			ga := Min(&in)
			assert.Equal(t, a, ga)
		}
	}
}

func TestMin2(t *testing.T) {
	for a := 0; a < 8; a++ {
		for b := 0; b < 8; b++ {
			if a == b {
				continue
			}

			in := [...]uint64{2, 2, 2, 2, 2, 2, 2, 2}
			in[a] = 0
			in[b] = 1

			t.Log(in)
			ga, gb := Min2(&in)
			assert.Equal(t, a, ga)
			assert.Equal(t, b, gb)
		}
	}

	for b := 0; b < 8; b++ {
		for a := 0; a < b; a++ {
			for c := b; c < 8; c++ {
				in := [...]uint64{2, 2, 2, 2, 2, 2, 2, 2}
				in[a] = 0
				in[b] = 0
				in[c] = 0

				t.Log(in)
				ga, gb := Min2(&in)
				assert.Equal(t, a, ga)
				assert.Equal(t, b, gb)
			}
		}
	}
}

func BenchmarkSort(b *testing.B) {
	x := &[...]uint64{5, 7, 0, 2, 6, 1, 3, 4}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		Sort(x)
	}
}

func BenchmarkMin(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		Min(vals(i))
	}
}

func BenchmarkMin2(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		Min2(vals(i))
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
