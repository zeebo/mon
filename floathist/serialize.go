package floathist

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/zeebo/errs"
	"github.com/zeebo/mon/internal/buffer"
)

func (h *Histogram) Serialize(mem []byte) []byte {
	le := binary.LittleEndian

	if cap(mem) < 64 {
		mem = make([]byte, 0, 64)
	}
	buf := buffer.Of(mem)

	bm := h.l0.bm.Clone()

	buf = buf.Grow()
	le.PutUint32(buf.Front4()[:], bm.UnsafeUint32())
	buf = buf.Advance(4)

	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := (*level1)(atomic.LoadPointer((*ptr)(ptr(&h.l0.l1[i]))))

		bm := l1.bm.Clone()

		buf = buf.Grow()
		le.PutUint32(buf.Front4()[:], bm.UnsafeUint32())
		buf = buf.Advance(4)

		for {
			i, ok := bm.Next()
			if !ok {
				break
			}

			l2 := (*level2)(atomic.LoadPointer((*ptr)(ptr(&l1.l2[i]))))
			var bm b32

			buf = buf.Grow()
			pos := buf.Pos()
			buf = buf.Advance(4)

			for i := 0; i < levelSize; i++ {
				val := atomic.LoadUint64(&l2[i])
				if val == 0 {
					continue
				}

				bm.UnsafeSet(uint(i))

				buf = buf.Grow()
				nbytes := varintAppend(buf.Front9(), val)
				buf = buf.Advance(nbytes)
			}

			le.PutUint32(buf.Index4(pos)[:], bm.UnsafeUint32())

		}
	}

	return buf.Prefix()
}

func (h *Histogram) Load(data []byte) (err error) {
	le := binary.LittleEndian
	buf := buffer.OfLen(data)

	var bm0 b32
	var bm1 b32
	var bm2 b32

	if buf.Remaining() < 4 {
		err = errs.New("buffer too short")
		goto done
	}

	h.l0.bm.UnsafeSetUint32(le.Uint32(buf.Front4()[:]))
	buf = buf.Advance(4)

	bm0 = h.l0.bm.UnsafeClone()

	for {
		i, ok := bm0.Next()
		if !ok {
			break
		}

		l1 := new(level1)
		h.l0.l1[i] = l1

		if buf.Remaining() < 4 {
			err = errs.New("buffer too short")
			goto done
		}

		l1.bm.UnsafeSetUint32(le.Uint32(buf.Front4()[:]))
		buf = buf.Advance(4)

		bm1 = l1.bm.UnsafeClone()

		for {
			i, ok := bm1.Next()
			if !ok {
				break
			}

			l2 := new(level2)
			l1.l2[i] = l2

			if buf.Remaining() < 4 {
				err = errs.New("buffer too short")
				goto done
			}

			bm2.UnsafeSetUint32(le.Uint32(buf.Front4()[:]))
			buf = buf.Advance(4)

			for {
				i, ok := bm2.Next()
				if !ok {
					break
				}

				if rem := buf.Remaining(); rem >= 9 {
					var nbytes uintptr
					nbytes, l2[i] = fastVarintConsume(buf.Front9())
					if nbytes > rem {
						err = errs.New("invalid varint data")
						goto done
					}
					buf = buf.Advance(nbytes)

				} else {
					l2[i], buf, ok = safeVarintConsume(buf)
					if !ok {
						err = errs.New("invalid varint data")
						goto done
					}
				}
			}
		}
	}

done:
	return err
}
