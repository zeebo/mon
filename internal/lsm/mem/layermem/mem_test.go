package layermem

import (
	"fmt"
	"testing"

	"github.com/zeebo/mon/internal/lsm/mem/testmem"
)

func TestMem(t *testing.T) {
	var m T
	m.Init(testmem.MemCap)

	for i := 0; i < 100; i++ {
		m.SetString("00000000000000000000000000000000"+fmt.Sprint(i), nil)
	}

	iter := m.Iters()[0]
	for iter.Next() {
		t.Log(string(iter.Key()))
	}
}

func BenchmarkLayerMem(b *testing.B) {
	testmem.Benchmark(b, func(cap uint64) testmem.T {
		m := new(T)
		m.Init(cap)
		return m
	})
}
