package flat

import (
	"testing"

	. "github.com/zeebo/mon/internal/tests"
)

func BenchmarkFlat(b *testing.B) { RunBenchmarks(b, func() Type { return new(Table) }) }
