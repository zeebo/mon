package inthist

import (
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/mon/internal/bitmap"
	"golang.org/x/sys/cpu"
)

//go:noescape
func sumHistogramAVX2(data *[64]uint32) uint64

// sumHistogram is either backed by AVX2 or a partially unrolled loop.
var sumHistogram = map[bool]func(*[64]uint32) uint64{
	true:  sumHistogramAVX2,
	false: sumHistogramSlow,
}[cpu.X86.HasAVX2]

// sumHistogramSlow sums the histogram buffers using an unrolled loop.
func sumHistogramSlow(buf *[64]uint32) (total uint64) {
	for i := 0; i <= 56; i += 8 {
		total += 0 +
			uint64(buf[i+0]) +
			uint64(buf[i+1]) +
			uint64(buf[i+2]) +
			uint64(buf[i+3]) +
			uint64(buf[i+4]) +
			uint64(buf[i+5]) +
			uint64(buf[i+6]) +
			uint64(buf[i+7])
	}
	return total
}

const ( // histEntriesBits of 6 keeps ~1.5% error.
	histEntriesBits = 6
	histBuckets     = 63 - histEntriesBits
	histEntries     = 1 << histEntriesBits // 64
)

// histBucket is the type of a histogram bucket.
type histBucket struct {
	entries [histEntries]uint32
}

// loadBucket atomically loads the bucket pointer from the address.
func loadBucket(addr **histBucket) *histBucket {
	return (*histBucket)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(addr))))
}

// casBucket atomically compares and swaps the bucket pointer into the address.
func casBucket(addr **histBucket, old, new *histBucket) bool {
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(addr)),
		unsafe.Pointer(old), unsafe.Pointer(new))
}

// lowerValue returns the smallest value that can be stored at the entry.
func lowerValue(bucket, entry uint64) int64 {
	return (1<<bucket-1)<<histEntriesBits + int64(entry<<bucket)
}

// upperValue returns the largest value that can be stored at the entry (inclusive).
func upperValue(bucket, entry uint64) int64 {
	return (1<<bucket-1)<<histEntriesBits + int64(entry<<bucket) + (1<<bucket - 1)
}

// middleBase returns the base offset for finding the middleValue for a bucket.
func middleBase(bucket uint64) float64 {
	return float64((int64(1)<<bucket-1)<<histEntriesBits) +
		float64((int64(1)<<bucket)-1)/2
}

// middleOffset returns the amount to add to the appropriate middleBase to get the
// middleValue for some bucket and entry.
func middleOffset(bucket, entry uint64) float64 {
	return float64(int64(entry << bucket))
}

// bucketEntry returns the bucket and entry that should contain the value v.
func bucketEntry(v int64) (bucket, entry uint64) {
	uv := uint64(v + histEntries)
	bucket = uint64(bits.Len64(uv)) - histEntriesBits - 1
	return bucket % 64, (uv>>bucket - histEntries) % histEntries
}

// Histogram keeps track of an exponentially increasing range of buckets
// so that there is a consistent relative error per bucket.
type Histogram struct {
	bitmap  bitmap.B64      // encodes which buckets are set
	buckets [64]*histBucket // 64 so that bounds checks can be removed easier
}

// Observe records the value in the histogram.
func (h *Histogram) Observe(v int64) {
	// upperValue is inlined and constant folded
	if v < 0 || v > upperValue(histBuckets-1, histEntries-1) {
		return
	}

	bucket, entry := bucketEntry(v)

	b := loadBucket(&h.buckets[bucket])
	if b == nil {
		b = new(histBucket)
		if !casBucket(&h.buckets[bucket], nil, b) {
			b = loadBucket(&h.buckets[bucket])
		} else {
			h.bitmap.Set(uint(bucket))
		}
	}

	atomic.AddUint32(&b.entries[entry], 1)
}

// Total returns the number of completed calls.
func (h *Histogram) Total() (total int64) {
	bm := h.bitmap.Clone()
	for {
		bucket, ok := bm.Next()
		if !ok {
			return total
		}
		total += int64(sumHistogram(&loadBucket(&h.buckets[bucket]).entries))
	}
}

// For quantile, we compute a target value at the start. After that, when
// walking the counts, we are sure we'll still hit the target since the
// counts and totals monotonically increase. This means that the returned
// result might be slightly smaller than the real result, but since
// the call is so fast, it's unlikely to drift very much.

// Quantile returns an estimation of the qth quantile in [0, 1].
func (h *Histogram) Quantile(q float64) int64 {
	target, acc := uint64(q*float64(h.Total())+0.5), uint64(0)

	bm := h.bitmap.Clone()
	for {
		bucket, ok := bm.Next()
		if !ok {
			return upperValue(histBuckets-1, histEntries-1)
		}

		b := loadBucket(&h.buckets[bucket])
		bacc := acc + sumHistogram(&b.entries)
		if bacc < target {
			acc = bacc
			continue
		}

		for entry := range b.entries[:] {
			acc += uint64(atomic.LoadUint32(&b.entries[entry]))
			if acc >= target {
				base := middleBase(uint64(bucket))
				return int64(base + 0.5 + middleOffset(uint64(bucket), uint64(entry)))
			}
		}
	}
}

// CDF returns an estimate for what quantile the value v is.
func (h *Histogram) CDF(v int64) float64 {
	var sum, total int64
	vbucket, ventry := bucketEntry(v)
	bm := h.bitmap.Clone()
	for {
		bucket, ok := bm.Next()
		if !ok {
			return float64(sum) / float64(total)
		}

		entries := &loadBucket(&h.buckets[bucket]).entries
		bucketSum := int64(sumHistogram(entries))
		total += bucketSum

		if uint64(bucket) < vbucket {
			sum += bucketSum
		} else if uint64(bucket) == vbucket {
			for i := uint64(0); i <= ventry; i++ {
				sum += int64(entries[i])
			}
		}
	}
}

// When computing the average or variance, we don't do any locking.
// When we have finished adding up into the accumulator, we know the
// actual statistic has to be somewhere between acc / stotal and
// acc / etotal, because the counts and totals monotonically increase.
// We return the average of those bounds. Since we're dominated by
// cache misses, this doesn't cost much extra.

// Sum returns an estimation of the sum.
func (h *Histogram) Sum() float64 {
	var values float64

	bm := h.bitmap.Clone()
	for {
		bucket, ok := bm.Next()
		if !ok {
			return values
		}

		b := loadBucket(&h.buckets[bucket])
		base := middleBase(uint64(bucket))

		for entry := range b.entries[:] {
			if count := float64(atomic.LoadUint32(&b.entries[entry])); count > 0 {
				value := base + middleOffset(uint64(bucket), uint64(entry))
				values += count * value
			}
		}
	}
}

// Average returns an estimation of the sum and average.
func (h *Histogram) Average() (float64, float64) {
	var values, total float64

	bm := h.bitmap.Clone()
	for {
		bucket, ok := bm.Next()
		if !ok {
			if total == 0 {
				return 0, 0
			}
			return values, values / total
		}

		b := loadBucket(&h.buckets[bucket])
		base := middleBase(uint64(bucket))

		for entry := range b.entries[:] {
			if count := float64(atomic.LoadUint32(&b.entries[entry])); count > 0 {
				value := base + middleOffset(uint64(bucket), uint64(entry))
				values += count * value
				total += count
			}
		}
	}
}

// Variance returns an estimation of the sum, average and variance.
func (h *Histogram) Variance() (float64, float64, float64) {
	var values, total, total2, mean, vari float64

	bm := h.bitmap.Clone()
	for {
		bucket, ok := bm.Next()
		if !ok {
			if total == 0 {
				return 0, 0, 0
			}
			return values, values / total, vari / total
		}

		b := loadBucket(&h.buckets[bucket])
		base := middleBase(uint64(bucket))

		for entry := range b.entries[:] {
			if count := float64(atomic.LoadUint32(&b.entries[entry])); count > 0 {
				value := base + middleOffset(uint64(bucket), uint64(entry))
				values += count * value
				total += count
				total2 += count * count
				mean_ := mean
				mean += (count / total) * (value - mean_)
				vari += count * (value - mean_) * (value - mean)
			}
		}
	}
}

// Percentiles returns calls the callback with information about the CDF.
// The total may increase during the call, but it should never be less
// than the count.
func (h *Histogram) Percentiles(cb func(value, count, total int64)) {
	acc, total := int64(0), h.Total()

	bm := h.bitmap.Clone()
	for {
		bucket, ok := bm.Next()
		if !ok {
			return
		}

		b := loadBucket(&h.buckets[bucket])
		for entry := range b.entries[:] {
			if count := int64(atomic.LoadUint32(&b.entries[entry])); count > 0 {
				if acc == 0 {
					cb(lowerValue(uint64(bucket), uint64(entry)), 0, total)
				}
				acc += count
				if acc > total {
					total = h.Total()
				}
				cb(upperValue(uint64(bucket), uint64(entry)), acc, total)
			}
		}
	}
}
