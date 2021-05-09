package inthist

import (
	"encoding/binary"

	"github.com/zeebo/errs"
	"github.com/zeebo/mon/internal/buffer"
)

func (h *Histogram) Serialize(dst []byte) []byte {
	le := binary.LittleEndian

	if cap(dst) < 128 {
		dst = make([]byte, 128)
	}

	buf := buffer.Of(dst).Advance(8)
	aidx := uintptr(0)

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

			if skip > 0 {
				if acount == 64 {
					le.PutUint64(buf.Index8(aidx)[:], action)
					aidx = buf.Pos()
					buf = buf.Advance(8).Grow()
					acount = 0
				}

				action = action>>1 | (1 << 63)
				acount++

				nbytes, enc := varintStats(skip)
				le.PutUint64(buf.Front8()[:], enc)
				buf = buf.Advance(uintptr(nbytes)).Grow()
				skip = 0
			}

			{
				if acount == 64 {
					le.PutUint64(buf.Index8(aidx)[:], action)
					aidx = buf.Pos()
					buf = buf.Advance(8).Grow()
					acount = 0
				}

				action = action >> 1
				acount++

				delta := int32(count) - int32(prev)
				val := uint32((delta + delta) ^ (delta >> 31))

				nbytes, enc := varintStats(val)
				le.PutUint64(buf.Front8()[:], enc)
				buf = buf.Advance(uintptr(nbytes)).Grow()
			}

			prev = count
		}
	}

	if delta := histBuckets - prevBucket; delta > 1 {
		skip += histEntries * uint32(delta-1)
	}

	if skip > 0 {
		if acount == 64 {
			le.PutUint64(buf.Index8(aidx)[:], action)
			aidx = buf.Pos()
			buf = buf.Advance(8).Grow()
			acount = 0
		}

		action = action>>1 | (1 << 63)
		acount++

		nbytes, enc := varintStats(skip)
		le.PutUint64(buf.Front8()[:], enc)
		buf = buf.Advance(uintptr(nbytes)).Grow()
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

	b := (*histBucket)(nil)

	bi := uint32(0)
	entry := uint32(0)
	value := uint32(0)

	for buf.Remaining() > 8 {
		actions := le.Uint64(buf.Front8()[:])
		buf = buf.Advance(8)

		for i := 0; i < 64; i++ {
			var dec uint32

			rem := buf.Remaining()
			if rem >= 8 {
				var nbytes uint8
				nbytes, dec = fastVarintConsume(le.Uint64(buf.Front8()[:]))
				buf = buf.Advance(uintptr(nbytes))
				if buf.Pos() > buf.Cap() {
					err = errs.New("invalid varint data")
					goto done
				}

			} else if rem > 0 {
				var ok bool
				dec, buf, ok = safeVarintConsume(buf)
				if !ok {
					err = errs.New("invalid varint data")
					goto done
				}

			} else {
				goto check

			}

			if actions&1 != 0 {
				entry += dec

			} else {
				delta := (dec >> 1) ^ -(dec & 1)
				value += delta

				if b == nil {
					if int(bi) >= histBuckets {
						err = errs.New("overflow number of buckets")
						goto done
					}

					b = new(histBucket)
					h.buckets[bi] = b
					h.bitmap.Set(uint(bi))
				}

				b.entries[entry%histEntries] = value
				entry++
			}

			if entry >= histEntries {
				bi += entry / histEntries
				entry %= histEntries
				b = nil
			}

			actions >>= 1
		}
	}

check:
	if bi != histBuckets || entry != 0 || buf.Remaining() != 0 {
		err = errs.New("invalid encoded data")
	}

done:
	return err
}
