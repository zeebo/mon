package mon

import (
	"errors"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/this"
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

		Collect(func(name string, his *State) bool { return true })

		Collect(func(_ string, _ *State) bool {
			assert.That(t, false)
			return true
		})
	})
}

func BenchmarkNanotime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = nanotime()
	}
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

	b.Run("This", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			func() {
				timer := StartNamed(this.This())
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

	b.Run("Thunk", func(b *testing.B) {
		b.ReportAllocs()
		var thunk Thunk

		for i := 0; i < b.N; i++ {
			func() {
				timer := thunk.Start()
				defer timer.Stop(nil)
			}()
		}
	})

	b.Run("WithError", func(b *testing.B) {
		err := errors.New("some error: whatever")
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			func() {
				timer := Start()
				defer timer.Stop(&err)
			}()
		}
	})
}
