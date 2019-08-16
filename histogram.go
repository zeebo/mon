package mon

import (
	"encoding/binary"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/mon/internal/buffer"
)

const ( // histEntriesBits of 8 keeps ~0.3% error.
	histEntriesBits = 8
	histBuckets     = 63 - histEntriesBits
	histEntries     = 1 << histEntriesBits
)

// histBucket is the type of a histogram bucket.
type histBucket [histEntries]int32

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
func lowerValue(bucket, entry uint64) int64 {
	return (1<<bucket-1)<<histEntriesBits + int64(entry<<bucket)
}

// upperValue returns the largest value that can be stored at the entry (inclusive).
func upperValue(bucket, entry uint64) int64 {
	return (1<<bucket-1)<<histEntriesBits + int64(entry<<bucket) + (1<<bucket - 1)
}

// middleValue returns the value between the smallest and largest that can be
// stored at the entry.
func middleValue(bucket, entry uint64) float64 {
	return middleBase(bucket) + middleOffset(bucket, entry)
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
	return bucket, uv>>bucket - histEntries
}

// Histogram keeps track of an exponentially increasing range of buckets
// so that there is a consistent relative error per bucket.
type Histogram struct {
	total  int64
	counts [histBuckets]*histBucket
}

// Observe records the value in the histogram.
func (h *Histogram) Observe(v int64) {
	// upperValue is inlined and constant folded
	if v < 0 || v > upperValue(histBuckets-1, histEntries-1) {
		return
	}

	bucket, entry := bucketEntry(v)
	if bucket < histBuckets && entry < histEntries {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			b = h.makeBucket(bucket)
		}
		atomic.AddInt64(&h.total, 1)
		atomic.AddInt32(&b[entry], 1)
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
	target, acc := uint64(q*float64(h.Total())+0.5), uint64(0)

	for bucket := range h.counts[:] {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			continue
		}

		for entry := range b {
			acc += uint64(atomic.LoadInt32(&b[entry]))
			if acc >= target {
				return lowerValue(uint64(bucket), uint64(entry))
			}
		}
	}

	return upperValue(histBuckets-1, histEntries-1)
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

	for bucket := range h.counts[:] {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			continue
		}

		base := middleBase(uint64(bucket))
		for entry := range b {
			if count := float64(atomic.LoadInt32(&b[entry])); count > 0 {
				value := base + middleOffset(uint64(bucket), uint64(entry))
				values += count * value
			}
		}
	}

	return values
}

// Average returns an estimation of the sum and average.
func (h *Histogram) Average() (float64, float64) {
	var values, total float64

	for bucket := range h.counts[:] {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			continue
		}

		base := middleBase(uint64(bucket))
		for entry := range b {
			if count := float64(atomic.LoadInt32(&b[entry])); count > 0 {
				value := base + middleOffset(uint64(bucket), uint64(entry))
				values += count * value
				total += count
			}
		}
	}

	return values, values / total
}

// Variance returns an estimation of the sum, average and variance.
func (h *Histogram) Variance() (float64, float64, float64) {
	var values, total, total2, mean, vari float64

	for bucket := range h.counts[:] {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			continue
		}

		base := middleBase(uint64(bucket))
		for entry := range b {
			if count := float64(atomic.LoadInt32(&b[entry])); count > 0 {
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

	return values, values / total, vari / total
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
			if count := int64(atomic.LoadInt32(&b[entry])); count > 0 {
				acc += count
				cb(upperValue(uint64(bucket), uint64(entry)), acc, h.Total())
			}
		}
	}
}

// Serialize appends to dst a binary representation of the histogram.
func (h *Histogram) Serialize(dst []byte) []byte {
	var le = binary.LittleEndian

	if cap(dst) < 128 {
		dst = make([]byte, 128)
	}

	// leave room for 2 bytes at the start
	buf := buffer.Of(dst).Advance(2)

	// TODO(jeff): maybe we want to avoid 464 bytes on the stack
	// write all the bucket counts and keep track of which counts were set
	var counts [histBuckets]uint64
	prev := int32(0)

	for bucket := range h.counts[:] {
		b := loadBucket(&h.counts[bucket])
		if b == nil {
			continue
		}

		for entry := range b {
			count := atomic.LoadInt32(&b[entry])
			if count == 0 {
				continue
			}
			counts[bucket] |= 1 << uint(entry)

			delta := count - prev
			val := uint32(delta<<1) ^ uint32(delta>>31)
			prev = count

			{ // do a varint append
				nbytes, val := varintStats(val)
				buf = buf.Grow()
				le.PutUint64(buf.Front()[:], val)
				buf = buf.Advance(uintptr(nbytes))
			}
		}
	}

	// store the length of the counts at the start of the buffer
	le.PutUint16((*[2]byte)(buf.Base())[:], uint16(buf.Pos()))

	// write out RLE of bits in counts
	flip := false
	numZero := 0

nextCount:
	for _, v := range &counts {
		valZero := 0
		if flip {
			v = ^v
		}

		for {
			zero := bits.TrailingZeros64(v)
			valZero += zero
			numZero += zero

			if valZero >= 64 {
				numZero -= valZero - 64
				continue nextCount
			}

			{ // do a varint append
				nbytes, val := varintStats(uint32(numZero))
				buf = buf.Grow()
				le.PutUint64(buf.Front()[:], val)
				buf = buf.Advance(uintptr(nbytes))
			}

			numZero = 0
			flip = !flip
			v = ^v >> (uint(zero) % 64)
		}
	}

	{ // do a varint append
		nbytes, val := varintStats(uint32(numZero))
		buf = buf.Grow()
		le.PutUint64(buf.Front()[:], val)
		buf = buf.Advance(uintptr(nbytes))
	}

	return buf.Prefix()
}
