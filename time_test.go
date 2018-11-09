package mon

import (
	"testing"

	"github.com/zeebo/wosl/internal/assert"
)

func TestTime(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		StartNamed("foo").Stop()

		Times(func(name string, his *Histogram) bool {
			assert.Equal(t, name, "foo")
			assert.Equal(t, his.Total(), 1)
			return true
		})

		StartNamed("foo").Stop()

		Times(func(name string, his *Histogram) bool {
			assert.Equal(t, name, "foo")
			assert.Equal(t, his.Total(), 2)
			return true
		})

		StartNamed("bar").Stop()

		Times(func(name string, his *Histogram) bool {
			switch name {
			case "foo":
				assert.Equal(t, his.Total(), 2)
			case "bar":
				assert.Equal(t, his.Total(), 1)
			default:
				t.Fatal("invalid name:", name)
			}
			return true
		})
	})
}

func BenchmarkTime(b *testing.B) {
	b.Run("Auto", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			func() { defer Start().Stop() }()
		}
	})

	b.Run("Named", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			func() { defer StartNamed("bench").Stop() }()
		}
	})

	b.Run("NoDefer", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			timer := StartNamed("bench")
			timer.Stop()
		}
	})

	b.Run("ThunkNoDefer", func(b *testing.B) {
		b.ReportAllocs()
		var thunk Thunk

		for i := 0; i < b.N; i++ {
			timer := thunk.Start()
			timer.Stop()
		}
	})
}
