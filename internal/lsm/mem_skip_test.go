package lsm

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestSkipMem(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var rng pcg.T
		s := new(skipMem)
		s.init(1024)

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
			s.reset()

			for i := 0; i < 1000; i++ {
				s.SetString(fmt.Sprint(rng.Uint32n(100)), []byte(fmt.Sprint(i)))
			}
			s.SetString("4", []byte("99"))
			// dump()

			it := s.iter()
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
	benchmarkMem(b, func(cap uint64) testMem {
		m := new(skipMem)
		m.init(cap)
		return m
	})
}
