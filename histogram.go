package mon

import (
	"math/bits"
	"sync/atomic"
	"unsafe"
)

const ( // histEntriesBits of 6 keeps ~1.5% error.
	histEntriesBits = 6
	histBuckets     = 64 - histEntriesBits
	histEntries     = 1 << histEntriesBits
)

// histBucket is the type of a histogram bucket.
type histBucket [histEntries]int64

// loadBucket atomically loads the bucket pointer from the address.
func loadBucket(addr **histBucket) *histBucket {
	return (*histBucket)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(addr))))
}

// storeBucket atomically stores the bucket pointer into the address.
func storeBucket(addr **histBucket, val *histBucket) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(addr)), unsafe.Pointer(val))
}

// casBucket atomically compares and swaps the bucket pointer into the address.
func casBucket(addr **histBucket, old, new *histBucket) bool {
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(addr)),
		unsafe.Pointer(old), unsafe.Pointer(new))
}

// lowerValue returns the smallest value that can be stored at the entry.
func lowerValue(bucket uint, entry int) int64 {
	return (1<<bucket-1)<<histEntriesBits + int64(entry<<bucket)
}

// middleValue returns the value between the smallest and largest that can be
// stored at the entry.
func middleValue(bucket uint, entry int) int64 {
	return (1<<bucket-1)<<histEntriesBits + int64(entry<<bucket) + (1 << bucket / 2)
}

// upperValue returns the largest value that can be stored at the entry.
func upperValue(bucket uint, entry int) int64 {
	return (1<<bucket-1)<<histEntriesBits + int64(entry<<bucket) + (1 << bucket)
}

// Histogram keeps track of an exponentially increasing range of buckets
// so that there is a consistent relative error per bucket.
type Histogram struct {
	total  int64
	counts [histBuckets]*histBucket
}

// Observe records the value in the histogram.
func (h *Histogram) Observe(v int64) {
	v += histEntries
	bucket := uint64(bits.Len64(uint64(v))) - histEntriesBits - 1
	entry := uint64(v>>bucket) - histEntries

	if bucket < histBuckets && entry < histEntries {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			b = h.makeBucket(bucket)
		}
		atomic.AddInt64(&b[entry], 1)
		atomic.AddInt64(&h.total, 1)
	}
}

// makeBucket ensures the bucket exists and returns it.
func (h *Histogram) makeBucket(bucket uint64) *histBucket {
	b := loadBucket(&h.counts[bucket])
	if b == nil {
		casBucket(&h.counts[bucket], nil, new(histBucket))
		b = loadBucket(&h.counts[bucket])
	}
	return b
}

// Total returns the number of completed calls.
func (h *Histogram) Total() int64 { return atomic.LoadInt64(&h.total) }

// For quantile, we compute a target value at the start. After that, when
// walking the counts, we are sure we'll still hit the target since the
// counts and totals monotonically increase. This means that the returned
// result might be slightly smaller than the real result, but since
// the call is so fast, it's unlikely to drift very much.

// Quantile returns an estimation of the qth quantile in [0, 1].
func (h *Histogram) Quantile(q float64) int64 {
	target, acc := int64(q*float64(h.Total())+0.5), int64(0)

	for bucket := range h.counts[:] {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			continue
		}

		for entry := range b {
			acc += atomic.LoadInt64(&b[entry])
			if acc >= target {
				return lowerValue(uint(bucket), entry)
			}
		}
	}

	return upperValue(histBuckets, histEntries)
}

// When computing the average or variance, we don't do any locking.
// When we have finished adding up into the accumulator, we know the
// actual statistic has to be somewhere between acc / stotal and
// acc / etotal, because the counts and totals monotonically increase.
// We return the average of those bounds. Since we're dominated by
// cache misses, this doesn't cost much extra.

// Sum returns an estimation of the sum.
func (h *Histogram) Sum() float64 {
	acc := int64(0)

	for bucket := range h.counts[:] {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			continue
		}

		for entry := range b {
			if count := atomic.LoadInt64(&b[entry]); count > 0 {
				acc += count * middleValue(uint(bucket), entry)
			}
		}
	}

	return float64(acc)
}

// Average returns an estimation of the sum and average.
func (h *Histogram) Average() (float64, float64) {
	stotal, acc := float64(h.Total()), int64(0)

	for bucket := range h.counts[:] {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			continue
		}

		for entry := range b {
			if count := atomic.LoadInt64(&b[entry]); count > 0 {
				acc += count * middleValue(uint(bucket), entry)
			}
		}
	}

	etotal, facc := float64(h.Total()), float64(acc)
	return facc, (facc/stotal + facc/etotal) / 2
}

// Variance returns an estimation of the sum, average and variance.
func (h *Histogram) Variance() (float64, float64, float64) {
	stotal, acc := float64(h.Total()), 0.0
	sum, avg := h.Average()

	for bucket := range h.counts[:] {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			continue
		}

		for entry := range b {
			if count := atomic.LoadInt64(&b[entry]); count > 0 {
				dev := float64(middleValue(uint(bucket), entry)) - avg
				acc += dev * dev * float64(count)
			}
		}
	}

	etotal, facc := float64(h.Total()), float64(acc)
	return sum, avg, (facc/stotal + facc/etotal) / 2
}

// Percentiles returns calls the callback with information about the CDF.
// The total may increase during the call, but it should never be less
// than the count.
func (h *Histogram) Percentiles(cb func(value, count, total int64)) {
	acc := int64(0)

	for bucket := range h.counts[:] {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			continue
		}

		for entry := range b {
			count := atomic.LoadInt64(&b[entry])
			if count > 0 {
				acc += count
				cb(upperValue(uint(bucket), entry), acc, h.Total())
			}
		}
	}
}
