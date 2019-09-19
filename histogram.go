package mon

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/errs"
	"github.com/zeebo/mon/internal/bitmap"
	"github.com/zeebo/mon/internal/buffer"
	"golang.org/x/sys/cpu"
)

//go:noescape
func sumHistogramAVX2(*[64]uint32) uint64

// sumHistogram is either backed by AVX2 or a partially unrolled loop.
var sumHistogram = map[bool]func(*[64]uint32) uint64{
	true:  sumHistogramAVX2,
	false: sumHistogramSlow,
}[cpu.X86.HasAVX2]

// sumHistogramSlow sums the histogram buffers using an unrolled loop.
func sumHistogramSlow(buf *[64]uint32) (total uint64) {
	for i := 0; i <= 56; i += 8 {
		total += uint64(buf[i+0])
		total += uint64(buf[i+1])
		total += uint64(buf[i+2])
		total += uint64(buf[i+3])
		total += uint64(buf[i+4])
		total += uint64(buf[i+5])
		total += uint64(buf[i+6])
		total += uint64(buf[i+7])
	}
	return total
}

const ( // histEntriesBits of 6 keeps ~1.5% error.
	histEntriesBits = 6
	histBuckets     = 63 - histEntriesBits
	histEntries     = 1 << histEntriesBits
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
	bitmap  bitmap.B64
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
				acc += count
				if acc > total {
					total = h.Total()
				}
				cb(upperValue(uint64(bucket), uint64(entry)), acc, total)
			}
		}
	}
}

func (h *Histogram) Serialize(dst []byte) []byte {
	le := binary.LittleEndian

	if cap(dst) < 128 {
		dst = make([]byte, 128)
	}

	buf := buffer.Of(dst)
	aidx := uintptr(0)
	buf = buf.Advance(8)

	acount := uint8(0)
	action := uint64(0)

	prev := uint32(0)
	skip := uint32(0)

	prevBucket := ^uint(0)

	bm := h.bitmap.Clone()
	for {
		bucket, ok := bm.Next()
		if !ok {
			break
		}

		if delta := bucket - prevBucket; delta > 1 {
			skip += histEntries * uint32(delta-1)
		}
		prevBucket = bucket

		b := h.buckets[bucket]
		for entry := range b.entries[:] {
			count := b.entries[entry]
			if count == 0 {
				skip++
				continue
			}

			buf = buf.Grow()

			if skip > 0 {
				if acount == 64 {
					le.PutUint64(buf.Index8(aidx)[:], action)
					aidx = buf.Pos()
					buf = buf.Advance(8)
					acount = 0
				}

				action = action>>1 | (1 << 63)
				acount++

				nbytes, enc := varintStats(skip)
				le.PutUint64(buf.Front()[:], enc)
				buf = buf.Advance(uintptr(nbytes))
				skip = 0
			}

			{
				if acount == 64 {
					le.PutUint64(buf.Index8(aidx)[:], action)
					aidx = buf.Pos()
					buf = buf.Advance(8)
					acount = 0
				}

				action = action >> 1
				acount++

				delta := int32(count) - int32(prev)
				val := uint32((delta + delta) ^ (delta >> 31))

				nbytes, enc := varintStats(val)
				le.PutUint64(buf.Front()[:], enc)
				buf = buf.Advance(uintptr(nbytes))
			}

			prev = count
		}
	}

	if delta := histBuckets - prevBucket; delta > 1 {
		skip += histEntries * uint32(delta-1)
	}

	if skip > 0 {
		buf = buf.Grow()

		if acount == 64 {
			le.PutUint64(buf.Index8(aidx)[:], action)
			aidx = buf.Pos()
			buf = buf.Advance(8)
			acount = 0
		}

		action = action>>1 | (1 << 63)
		acount++

		nbytes, enc := varintStats(skip)
		le.PutUint64(buf.Front()[:], enc)
		buf = buf.Advance(uintptr(nbytes))
	}

	if acount > 0 {
		action >>= (64 - acount) % 64
		le.PutUint64(buf.Index8(aidx)[:], action)
	}

	return buf.Prefix()
}

func (h *Histogram) Load(data []byte) (err error) {
	le := binary.LittleEndian
	buf := buffer.OfLen(data)

	bi := uint64(0)
	b := (*histBucket)(nil)

	entry := uint32(0)
	value := uint32(0)

	for buf.Remaining() > 8 {
		actions := le.Uint64(buf.Front()[:])
		buf = buf.Advance(8)

		for i := 0; i < 64; i++ {
			var dec uint32

			rem := buf.Remaining()
			if rem == 0 {
				goto check

			} else if rem >= 8 {
				var nbytes uint8
				nbytes, dec = fastVarintConsume(le.Uint64(buf.Front()[:]))
				buf = buf.Advance(uintptr(nbytes))

			} else {
				var ok bool
				dec, buf, ok = safeVarintConsume(buf)
				if !ok {
					err = errs.New("invalid varint data")
					goto done
				}
			}

			if actions != 0 && actions&1 == 1 {
				entry += dec
				if entry >= histEntries {
					bi += uint64(entry / histEntries)
					entry = entry % histEntries
					b = nil
				}

			} else {
				delta := (dec >> 1) ^ -(dec & 1)
				value += delta

				if b == nil {
					if bi >= histBuckets {
						err = errs.New("overflow number of buckets")
						goto done
					}
					b = h.buckets[bi]
					if b == nil {
						b = new(histBucket)
						h.buckets[bi] = b
						h.bitmap.Set(uint(bi))
					}
				}

				b.entries[entry%histEntries] += value
				entry++

				if entry == histEntries {
					bi++
					entry = 0
					b = nil
				}
			}

			actions >>= 1
		}
	}

check:
	if bi != histBuckets || entry != 0 || buf.Remaining() != 0 {
		err = errs.New("invalid encoded data (%d, %d, %d)", bi, entry, buf.Remaining())
	}

done:
	return err
}

func (h *Histogram) Bitmap() string {
	var lines []string
	for bucket := range h.buckets[:] {
		b := loadBucket(&h.buckets[bucket])
		if b == nil {
			lines = append(lines, strings.Repeat("0", histEntries))
			continue
		}

		var line []byte
		for entry := range b.entries[:] {
			count := atomic.LoadUint32(&b.entries[entry])
			if count == 0 {
				line = append(line, '0')
			} else {
				line = append(line, '1')
			}
		}
		lines = append(lines, string(line))
	}
	return strings.Join(lines, "\n") + "\n"
}

func (h *Histogram) Dump() {
	for bucket := range h.buckets[:] {
		b := loadBucket(&h.buckets[bucket])
		if b == nil {
			continue
		}

		for entry := range b.entries[:] {
			count := atomic.LoadUint32(&b.entries[entry])
			if count == 0 {
				continue
			}

			fmt.Printf("%d:%d\n", lowerValue(uint64(bucket), uint64(entry)), count)
		}
	}
}
