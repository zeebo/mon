package mon

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
)

func BenchmarkGetState(b *testing.B) {
	var sink *State
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sink = GetState("foo")
	}

	runtime.KeepAlive(sink)
}

func BenchmarkState(b *testing.B) {
	b.Run("Observe", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			GetState("bench").Histogram().Observe(1)
		}
	})

	b.Run("Observe_Parallel", func(b *testing.B) {
		var n uint64
		b.RunParallel(func(pb *testing.PB) {
			metric := fmt.Sprintf("bench-%d", atomic.AddUint64(&n, 1))
			for pb.Next() {
				GetState(metric).Histogram().Observe(1)
			}
		})
	})
}
