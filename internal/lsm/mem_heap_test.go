package lsm

import (
	"testing"
)

func BenchmarkHeapMem(b *testing.B) {
	benchmarkMem(b, func(cap uint64) testMem {
		m := new(heapMem)
		m.init(cap)
		return m
	})

}
