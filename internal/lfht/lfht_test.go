package lfht

import (
	"math"
	"runtime"
	"testing"

	. "github.com/zeebo/mon/internal/tests"
	"github.com/zeebo/pcg"
)

func TestBitmap(t *testing.T) {
	var b bitmap128

	for i := uint(0); i < 128; i++ {
		b.set(i)

		got, ok := b.next()
		if !ok || got != i {
			t.Fatal(i)
		}
		if b != (bitmap128{}) {
			t.Fatal(b)
		}
	}
}

func BenchmarkBitmap(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		idx := uint(0)
		for i := 0; i < b.N; i++ {
			bm := bitmap128{1, 0}
			idx, _ = bm.next()
		}
		runtime.KeepAlive(idx)
	})

	b.Run("NextAll", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b := bitmap128{math.MaxUint64, math.MaxUint64}
			for {
				_, ok := b.next()
				if !ok {
					break
				}
			}
		}
	})
}

func TestTable(t *testing.T) {
	var ta Table
	for i := uint32(0); i < 100; i++ {
		ta.Upsert(Key(i), Empty)
		if ta.Lookup(Key(i)) != Value {
			ta.dump()
			t.Fatal(i)
		}
	}
	for i := uint32(0); i < 100; i++ {
		if ta.Lookup(Key(i)) != Value {
			ta.dump()
			t.Fatal(i)
		}
	}
	for iter := ta.Iterator(); iter.Next(); {
		if ta.Lookup(iter.Key()) != iter.Value() {
			ta.dump()
			t.Fatal(iter.Key(), iter.Value())
		}
	}
}

func TestTable_Iterator(t *testing.T) {
	for i := 0; i < 100; i++ {
		var ta Table
		for i := uint32(0); i < 100; i++ {
			ta.Upsert(Key(i), Empty)
		}

		var (
			done  = make(chan struct{})
			count = make(chan int, runtime.GOMAXPROCS(-1))
		)

		for i := 0; i < cap(count); i++ {
			go func() {
				rng := pcg.New(pcg.Uint64())
				total := 0
			again:
				select {
				case <-done:
				default:
					ta.Upsert(Key(rng.Uint32n(Size)), Empty)
					total++
					runtime.Gosched()
					goto again
				}
				count <- total
			}()
		}

		got := make(map[string]struct{})
		for iter := ta.Iterator(); iter.Next(); {
			got[iter.Key()] = struct{}{}
			runtime.Gosched()
		}
		close(done)

		total := 0
		for i := 0; i < cap(count); i++ {
			total += <-count
		}

		for i := uint32(0); i < 100; i++ {
			if _, ok := got[Key(i)]; !ok {
				t.Fatal(total, len(got), i)
			}
		}
	}
}

func BenchmarkLFHT(b *testing.B) {
	RunBenchmarks(b, func() Type { return new(Table) })

	b.Run("Iterate", func(b *testing.B) {
		var ta Table
		for i := uint32(0); i < Size; i++ {
			ta.Upsert(Key(i), Empty)
		}
		b.ReportAllocs()
		b.ResetTimer()

		iter := ta.Iterator()
		for i := 0; i < b.N; i++ {
			if !iter.Next() {
				iter = ta.Iterator()
			}
		}
	})
}
