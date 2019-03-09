package mon

import (
	"runtime"
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
