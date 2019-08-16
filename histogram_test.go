package mon

import (
	"encoding/hex"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
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

	t.Run("Variance", func(t *testing.T) {
		h := new(Histogram)
		for i := int64(0); i < 1000; i++ {
			h.Observe(i)
		}

		sum, average, variance := h.Variance()

		assert.Equal(t, sum, float64(500000))
		assert.Equal(t, average, float64(500))
		assert.Equal(t, variance, float64(83391.328))
	})

	t.Run("Percentiles", func(t *testing.T) {
		h := new(Histogram)
		for i := int64(0); i < 1000; i++ {
			r := int64(pcg.Uint32n(1000))
			h.Observe(r * r)
		}

		h.Percentiles(func(value, count, total int64) {
			t.Log(value, count, total)
		})
	})

	t.Run("Serialize", func(t *testing.T) {
		h := new(Histogram)
		for i := int64(0); i < 1000; i++ {
			r := int64(pcg.Uint32n(1000) + 500)
			h.Observe(r)
		}

		data := h.Serialize(nil)
		t.Logf("%d\n%s", len(data), hex.Dump(data))
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
		for i := 0; i < 1000000; i++ {
			his.Observe(int64(pcg.Uint64() >> histEntriesBits))
		}
		assert.Equal(b, his.Total(), 1000000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			his.Quantile(pcg.Float64())
		}
	})

	b.Run("Sum", func(b *testing.B) {
		his := new(Histogram)
		for i := 0; i < 1000; i++ {
			his.Observe(int64(pcg.Uint32n(64)))
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
			his.Observe(int64(pcg.Uint32n(64)))
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
			his.Observe(int64(pcg.Uint32n(64)))
		}
		assert.Equal(b, his.Total(), 1000)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _, _ = his.Variance()
		}
	})

	b.Run("Serialize", func(b *testing.B) {
		h := new(Histogram)
		for i := int64(0); i < 10000000; i++ {
			r := int64(pcg.Uint32n(1000) + 500)
			h.Observe(r)
		}
		buf := h.Serialize(nil)

		b.SetBytes(int64(len(buf)))
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			h.Serialize(buf[:0])
		}
	})
}
