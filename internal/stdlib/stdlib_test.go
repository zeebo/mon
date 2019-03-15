package stdlib

import (
	"testing"

	. "github.com/zeebo/mon/internal/tests"
)

func BenchmarkStdlib(b *testing.B) { RunBenchmarks(b, func() Type { return New() }) }
