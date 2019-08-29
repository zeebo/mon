package mon

import (
	"encoding/hex"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestHistogram(t *testing.T) {
	t.Run("Walk", func(t *testing.T) {
		type key = [2]uint64

		var (
			bucket uint64
			entry  uint64
			value  int64

			bucketEntries = map[key]bool{}
		)

		for bucket < histBuckets && entry < histEntries {
			// we must be on a new bucket/entry combination
			assert.That(t, !bucketEntries[key{bucket, entry}])
			bucketEntries[key{bucket, entry}] = true

			// value is always lowerValue(bucket, entry)
			assert.Equal(t, value, lowerValue(bucket, entry))

			// bucketEntry(lowerValue(bucket, entry)) == bucket, entry
			lbucket, lentry := bucketEntry(lowerValue(bucket, entry))
			assert.Equal(t, bucket, uint(lbucket))
			assert.Equal(t, entry, int(lentry))

			// bucketEntry(upperValue(bucket, entry)) == bucket, entry
			ubucket, uentry := bucketEntry(upperValue(bucket, entry))
			assert.Equal(t, bucket, uint(ubucket))
			assert.Equal(t, entry, int(uentry))

			// upperValue(bucket, entry) + 1 is in the next bucket/entry
			value = upperValue(bucket, entry) + 1
			bucket, entry = bucketEntry(value)
		}

		// we must have hit every bucket/entry
		assert.Equal(t, len(bucketEntries), histBuckets*histEntries)
	})

	t.Run("Zero", func(t *testing.T) {
		h := new(Histogram)
		sum, average, variance := h.Variance()
		assert.Equal(t, sum, 0.0)
		assert.Equal(t, average, 0.0)
		assert.Equal(t, variance, 0.0)
	})

	t.Run("Boundaries", func(t *testing.T) {
		h := new(Histogram)

		h.Observe(0)
		assert.Equal(t, h.Total(), 1)

		h.Observe(-1)
		assert.Equal(t, h.Total(), 1)

		upper := upperValue(histBuckets-1, histEntries-1)

		h.Observe(upper)
		assert.Equal(t, h.Total(), 2)

		for upper++; upper > 0; upper++ {
			h.Observe(upper)
			assert.Equal(t, h.Total(), 2)
		}
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
		assert.Equal(t, h.Quantile(.25), 249)
		assert.Equal(t, h.Quantile(.5), 498)
		assert.Equal(t, h.Quantile(1), 996)
	})

	t.Run("Variance", func(t *testing.T) {
		h := new(Histogram)
		rsum := int64(0)
		for i := int64(0); i < 1000; i++ {
			h.Observe(i)
			rsum += i
		}

		sum, average, variance := h.Variance()

		assert.Equal(t, sum, 499500.0)
		assert.Equal(t, average, 499.5)
		assert.Equal(t, variance, 83332.832)
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
