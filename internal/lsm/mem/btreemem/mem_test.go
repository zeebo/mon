package btreemem

import (
	"testing"

	"github.com/zeebo/mon/internal/lsm/mem/testmem"
)

func BenchmarkBtreeMem(b *testing.B) {
	testmem.Benchmark(b, func(cap uint64) testmem.T {
		m := new(T)
		m.Init(cap)
		return m
	})
}
