package skipmem

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/lsm/mem/testmem"
	"github.com/zeebo/mon/internal/lsm/testutil"
	"github.com/zeebo/pcg"
)

func TestSkipMem(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var rng pcg.T
		s := new(T)
		s.Init(1024)

		dump := func() {
			fmt.Println()
			for i := range s.chunks[:1] {
				cur := s.chunks[i].cursor()
				for {
					val := cur.get()
					fmt.Println(i, s.ents[val.idx].Key(), s.ents[val.idx].Value())
					if !cur.right() {
						break
					}
				}
			}
		}
		_ = dump

		for j := 0; j < 3; j++ {
			s.Reset()

			for i := 0; i < 1000; i++ {
				s.SetString(fmt.Sprint(rng.Uint32n(100)), []byte(fmt.Sprint(i)))
				// dump()
			}
			s.SetString("4", []byte("99"))
			// dump()

			fmt.Println(Buckets)

			it := s.Iter()
			last := ""
			total := 0
			for it.Next() {
				ent := it.Entry()
				fmt.Println(ent.Key(), ent.Value(), string(it.Key()), string(it.Value()))
				assert.That(t, last < string(it.Key()))
				last = string(it.Key())
				total++
			}
			assert.NoError(t, it.Err())
			assert.Equal(t, total, 100)
		}
	})

	t.Run("Pointery", func(t *testing.T) {
		s := new(T)
		s.Init(1 << 10)

		for i := 0; ; i++ {
			if !s.SetBytes(testutil.GetKey(i), nil) {
				break
			}
		}

		indexes := make(map[*chunk][2]int)
		for level := 0; level < skipMemLevels; level++ {
			right := 0
			for cur := &s.chunks[level]; cur != nil; cur = cur.right {
				indexes[cur] = [2]int{level, right}
				right++
			}
		}

		for level := skipMemLevels - 1; level >= 0; level-- {
			fmt.Println(level)
			right := 0
			for cur := &s.chunks[level]; cur != nil; cur = cur.right {
				idx, ok := indexes[cur.down]
				fmt.Println("\t", right, idx, ok, cur.data[0].prefix)
				right++
			}
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
