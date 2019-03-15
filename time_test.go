package mon

import (
	"errors"
	"testing"

	"github.com/zeebo/assert"
)

func TestTime(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		StartNamed("foo").Stop(nil)

		Times(func(name string, his *State) bool {
			assert.Equal(t, name, "foo")
			assert.Equal(t, his.Total(), 1)
			return true
		})

		StartNamed("foo").Stop(nil)

		Times(func(name string, his *State) bool {
			assert.Equal(t, name, "foo")
			assert.Equal(t, his.Total(), 2)
			return true
		})

		StartNamed("bar").Stop(nil)

		Times(func(name string, his *State) bool {
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
			func() {
				timer := Start()
				defer timer.Stop(nil)
			}()
		}
	})

	b.Run("Named", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			func() {
				timer := StartNamed("bench")
				defer timer.Stop(nil)
			}()
		}
	})

	b.Run("NamedNoDefer", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			timer := StartNamed("bench")
			timer.Stop(nil)
		}
	})

	b.Run("ThunkNoDefer", func(b *testing.B) {
		b.ReportAllocs()
		var thunk Thunk

		for i := 0; i < b.N; i++ {
			timer := thunk.Start()
			timer.Stop(nil)
		}
	})

	b.Run("NamedNoDeferWithError", func(b *testing.B) {
		err := errors.New("some error: whatever")
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			timer := StartNamed("bench")
			timer.Stop(&err)
		}
	})
}
