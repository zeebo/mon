package skipmem

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/lsm/mem/testmem"
	"github.com/zeebo/pcg"
)

func TestSkipMem(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var rng pcg.T
		s := new(T)
		s.Init(1024)

		// dump := func() {
		// 	fmt.Println()
		// 	for i := 0; i < int(s.len)+2; i++ {
		// 		fmt.Printf("n:%-4d kptr:%v val:%-4d ptrs:%-4d\n",
		// 			i,
		// 			s.ents[i].kptr,
		// 			s.ents[i].val,
		// 			s.ptrs[i].ptrs[:s.max])
		// 	}
		// }

		for j := 0; j < 3; j++ {
			s.Reset()

			for i := 0; i < 1000; i++ {
				s.SetString(fmt.Sprint(rng.Uint32n(100)), []byte(fmt.Sprint(i)))
			}
			s.SetString("4", []byte("99"))
			// dump()

			it := s.Iter()
			last := ""
			for it.Next() {
				// ent := it.Entry()
				// fmt.Println(ent.Key(), ent.Value(), string(it.Key()), string(it.Value()))
				assert.That(t, last < string(it.Key()))
				last = string(it.Key())
			}
			assert.NoError(t, it.Err())
		}
	})
}

func BenchmarkSkipMem(b *testing.B) {
	testmem.Benchmark(b, func(cap uint64) testmem.T {
		m := new(T)
		m.Init(cap)
		return m
	})
}
