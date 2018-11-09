package mon

import (
	"testing"
	"time"

	"github.com/zeebo/wosl/internal/assert"
	"github.com/zeebo/wosl/internal/pcg"
)

func TestHistogram(t *testing.T) {
	t.Run("Largest", func(t *testing.T) {
		h := new(Histogram)

		h.start()
		h.done(int64(time.Hour))
		assert.Equal(t, h.Total(), 1)

		h.start()
		h.done(int64(2 * time.Hour))
		assert.Equal(t, h.Total(), 1)
	})

	t.Run("Basic", func(t *testing.T) {
		h := new(Histogram)

		for i := int64(0); i < 1000; i++ {
			h.start()
			h.done(i)
		}
	})

	t.Run("Quantile", func(t *testing.T) {
		h := new(Histogram)
		for i := int64(0); i < 1000; i++ {
			h.done(i)
		}

		assert.Equal(t, h.Quantile(0), 0)
		assert.Equal(t, h.Quantile(.25), 248)
		assert.Equal(t, h.Quantile(.5), 496)
		assert.Equal(t, h.Quantile(1), 992)
	})

	t.Run("Average", func(t *testing.T) {
		h := new(Histogram)
		for i := int64(0); i < 1000; i++ {
			h.done(i)
		}

		assert.Equal(t, h.Average(), float64(500))
	})
}

func BenchmarkHistogram(b *testing.B) {
	b.Run("Start+Done", func(b *testing.B) {
		his := new(Histogram)

		for i := 0; i < b.N; i++ {
			his.start()
			his.done(1)
		}
	})

	b.Run("Quantile", func(b *testing.B) {
		his := new(Histogram)
		rng := pcg.New(1, 1)
		for i := 0; i < 1000000; i++ {
			his.start()
			his.done(int64(rng.Uint32()<<28 | rng.Uint32()))
		}
		assert.Equal(b, his.Total(), 1000000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			his.Quantile(rng.Float64())
		}
	})

	b.Run("Average", func(b *testing.B) {
		his := new(Histogram)
		rng := pcg.New(1, 1)
		for i := 0; i < 1000; i++ {
			his.start()
			his.done(int64(rng.Intn(64)))
		}
		assert.Equal(b, his.Total(), 1000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = his.Average()
		}
	})

	b.Run("Variance", func(b *testing.B) {
		his := new(Histogram)
		rng := pcg.New(1, 1)
		for i := 0; i < 1000; i++ {
			his.start()
			his.done(int64(rng.Intn(64)))
		}
		assert.Equal(b, his.Total(), 1000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = his.Variance()
		}
	})

}
