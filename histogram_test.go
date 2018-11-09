package mon

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestHistogram(t *testing.T) {
	t.Run("Boundaries", func(t *testing.T) {
		h := new(Histogram)

		h.Observe(0)
		assert.Equal(t, h.Total(), 1)

		h.Observe(1<<63 - 1 - histEntries)
		assert.Equal(t, h.Total(), 2)

		h.Observe(-1)
		assert.Equal(t, h.Total(), 2)

		h.Observe(1<<63 - histEntries)
		assert.Equal(t, h.Total(), 2)
	})

	t.Run("Basic", func(t *testing.T) {
		h := new(Histogram)

		for i := int64(0); i < 1000; i++ {
			h.Observe(i)
		}
	})

	t.Run("Quantile", func(t *testing.T) {
		h := new(Histogram)
		for i := int64(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.Quantile(0), 0)
		assert.Equal(t, h.Quantile(.25), 248)
		assert.Equal(t, h.Quantile(.5), 496)
		assert.Equal(t, h.Quantile(1), 992)
	})

	t.Run("Average", func(t *testing.T) {
		h := new(Histogram)
		for i := int64(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.Average(), float64(500))
	})

	t.Run("Percentiles", func(t *testing.T) {
		h := new(Histogram)
		rng := newPCG(1, 1)
		for i := 0; i < 1000; i++ {
			r := int64(rng.Intn(1000))
			h.Observe(r * r)
		}

		h.Percentiles(func(value, count, total int64) {
			t.Log(value, count, total)
		})
	})
}

func BenchmarkHistogram(b *testing.B) {
	b.Run("Observe", func(b *testing.B) {
		his := new(Histogram)

		for i := 0; i < b.N; i++ {
			his.Observe(1)
		}
	})

	b.Run("Quantile", func(b *testing.B) {
		his := new(Histogram)
		rng := newPCG(1, 1)
		for i := 0; i < 1000000; i++ {
			his.Observe(int64(rng.Uint32()<<28 | rng.Uint32()))
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
		rng := newPCG(1, 1)
		for i := 0; i < 1000; i++ {
			his.Observe(int64(rng.Intn(64)))
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
		rng := newPCG(1, 1)
		for i := 0; i < 1000; i++ {
			his.Observe(int64(rng.Intn(64)))
		}
		assert.Equal(b, his.Total(), 1000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = his.Variance()
		}
	})
}
