package floathist

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestQuantile(t *testing.T) {
	t.Run("Quantile", func(t *testing.T) {
		h := new(Histogram)
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.Quantile(0), 0.)
		assert.Equal(t, h.Quantile(.25), 248.)
		assert.Equal(t, h.Quantile(.5), 496.)
		assert.Equal(t, h.Quantile(1), 992.)
	})

	t.Run("CDF", func(t *testing.T) {
		h := new(Histogram)
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.CDF(0), 0.001)
		assert.Equal(t, h.CDF(250), 0.252)
		assert.Equal(t, h.CDF(500), 0.504)
		assert.Equal(t, h.CDF(1000), 1.0)
	})

	t.Run("Sum", func(t *testing.T) {
		h := new(Histogram)
		rsum := float32(0)
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
			rsum += i
		}

		assert.Equal(t, h.Sum(), 499978.6640625) // 499500
	})

	t.Run("Average", func(t *testing.T) {
		h := new(Histogram)
		rsum := float32(0)
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
			rsum += i
		}

		sum, avg := h.Average()

		assert.Equal(t, sum, 499978.6640625) // 499500
		assert.Equal(t, avg, 499.9786640625) // 499.5
	})

	t.Run("Variance", func(t *testing.T) {
		h := new(Histogram)
		rsum := float32(0)
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
			rsum += i
		}

		sum, avg, vari := h.Variance()

		assert.Equal(t, sum, 499978.6640625)   // 499500
		assert.Equal(t, avg, 499.9786640625)   // 499.5
		assert.Equal(t, vari, 83433.942757616) // 83416.667
	})
}

func BenchmarkHistogram(b *testing.B) {
	b.Run("Observe", func(b *testing.B) {
		his := new(Histogram)

		for i := 0; i < b.N; i++ {
			his.Observe(1)
		}
	})

	b.Run("Observe_Parallel", func(b *testing.B) {
		his := new(Histogram)
		n := int64(0)
		b.RunParallel(func(pb *testing.PB) {
			i := float32(uint64(1024) << uint64(atomic.AddInt64(&n, 1)))
			for pb.Next() {
				his.Observe(i)
			}
		})
	})

	b.Run("Total", func(b *testing.B) {
		his := new(Histogram)
		for i := 0; i < 1000000; i++ {
			his.Observe(pcg.Float32())
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			his.Total()
		}
	})

	b.Run("Total_Easy", func(b *testing.B) {
		his := new(Histogram)
		for i := 0; i < 1000000; i++ {
			his.Observe(math.Float32frombits(pcg.Uint32() | ((1<<10 - 1) << 22)))
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			his.Total()
		}
	})

	b.Run("Quantile", func(b *testing.B) {
		his := new(Histogram)
		for i := 0; i < 1000000; i++ {
			his.Observe(pcg.Float32())
		}
		assert.Equal(b, his.Total(), 1000000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			his.Quantile(pcg.Float64())
		}
	})

	b.Run("Quantile_Easy", func(b *testing.B) {
		his := new(Histogram)
		for i := 0; i < 1000000; i++ {
			his.Observe(math.Float32frombits(pcg.Uint32() | ((1<<10 - 1) << 22)))
		}
		assert.Equal(b, his.Total(), 1000000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			his.Quantile(pcg.Float64())
		}
	})

	b.Run("CDF", func(b *testing.B) {
		his := new(Histogram)
		for i := 0; i < 1000000; i++ {
			his.Observe(pcg.Float32())
		}
		assert.Equal(b, his.Total(), 1000000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			his.CDF(pcg.Float32())
		}
	})

	b.Run("CDF_Easy", func(b *testing.B) {
		his := new(Histogram)
		for i := 0; i < 1000000; i++ {
			his.Observe(math.Float32frombits(pcg.Uint32() | ((1<<10 - 1) << 22)))
		}
		assert.Equal(b, his.Total(), 1000000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			his.CDF(pcg.Float32())
		}
	})

	b.Run("Sum", func(b *testing.B) {
		his := new(Histogram)
		for i := 0; i < 1000; i++ {
			his.Observe(pcg.Float32())
		}
		assert.Equal(b, his.Total(), 1000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = his.Sum()
		}
	})

	b.Run("Average", func(b *testing.B) {
		his := new(Histogram)
		for i := 0; i < 1000; i++ {
			his.Observe(pcg.Float32())
		}
		assert.Equal(b, his.Total(), 1000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = his.Average()
		}
	})

	b.Run("Variance", func(b *testing.B) {
		his := new(Histogram)
		for i := 0; i < 1000; i++ {
			his.Observe(pcg.Float32())
		}
		assert.Equal(b, his.Total(), 1000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _, _ = his.Variance()
		}
	})
}
