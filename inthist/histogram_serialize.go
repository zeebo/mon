package inthist

import (
	"encoding/binary"

	"github.com/zeebo/errs"
	"github.com/zeebo/mon/internal/bitmap"
	"github.com/zeebo/mon/internal/buffer"
)

// TODO: try a different "varint" strategy where we write out 2 control bits
// per entry to encode the number of bytes. then it's just a shift based on
// the len32 of the thing. we can do a bucket's worth (64) in 16 bytes every
// time. it does mean 10 bits per entry, for a minimum of 80 bytes per bucket
// but it should be amenable to super fast avx2 implementations for both
// reading and writing which would be cool.

func (h *Histogram) Serialize(dst []byte) []byte {
	le := binary.LittleEndian

	if cap(dst) < 64 {
		dst = make([]byte, 64)
	}

	buf := buffer.Of(dst)
	bm := h.bitmap.Clone()

	le.PutUint64(buf.Front()[:], bm[0])
	buf = buf.Advance(8)

	for {
		bi, ok := bm.Next()
		if !ok {
			return buf.Grow().Advance(7).Prefix()
		}
		b := h.buckets[bi]

		for i := 0; i <= 60; i += 4 {
			nbytes0, enc0 := varintStats(b.entries[i+0])
			nbytes1, enc1 := varintStats(b.entries[i+1])
			nbytes2, enc2 := varintStats(b.entries[i+2])
			nbytes3, enc3 := varintStats(b.entries[i+3])

			le.PutUint64(buf.Front()[:], enc0)
			buf = buf.Advance(uintptr(nbytes0))
			le.PutUint64(buf.Front()[:], enc1)
			buf = buf.Advance(uintptr(nbytes1))
			le.PutUint64(buf.Front()[:], enc2)
			buf = buf.Advance(uintptr(nbytes2))
			le.PutUint64(buf.Front()[:], enc3)
			buf = buf.Advance(uintptr(nbytes3))
		}
	}
}

func (h *Histogram) Load(data []byte) (err error) {
	le := binary.LittleEndian
	buf := buffer.OfLen(data)
	var bm bitmap.B64

	if buf.Remaining() < 8 {
		goto err
	}
	bm[0] = le.Uint64(buf.Front()[:])
	buf = buf.Advance(8)

	for {
		bi, ok := bm.Next()
		if !ok {
			return nil
		}

		b := h.buckets[bi]
		if b == nil {
			b = new(histBucket)
			h.buckets[bi] = b
			h.bitmap.Set(uint(bi))
		}

		for i := 0; i < 64; i++ {
			if buf.Remaining() < 8 {
				goto err
			}

			if *buf.Head() == 0 {
				buf = buf.Advance(1)
				continue
			}

			nbytes, dec := fastVarintConsume(le.Uint64(buf.Front()[:]))
			buf = buf.Advance(uintptr(nbytes))
			b.entries[i] += dec
		}
	}

err:
	return errs.New("invalid buffer data")
}

func (h *Histogram) SerializeOld(dst []byte) []byte {
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

func (h *Histogram) LoadOld(data []byte) (err error) {
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
