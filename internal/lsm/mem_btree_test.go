package lsm

import (
	"testing"
)

func BenchmarkBtreeMem(b *testing.B) {
	benchmarkMem(b, func(cap uint64) testMem {
		m := new(btreeMem)
		m.init(cap)
		return m
	})

}
