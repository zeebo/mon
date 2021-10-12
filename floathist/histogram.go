package floathist

import (
	"math"
	"sync/atomic"
	"unsafe"
)

type ptr = unsafe.Pointer

const (
	levelShift = 5
	levelSize  = 1 << levelShift
	levelMask  = 1<<levelShift - 1
)

type (
	level0 struct {
		bm b32
		l1 [levelSize]*level1
	}
	level1 struct {
		bm b32
		l2 [levelSize]*level2
	}
	level2 [levelSize]uint64
)

type Histogram struct {
	l0 level0
}

func (h *Histogram) Observe(v float32) {
	if v != v || v > math.MaxFloat32 || v < -math.MaxFloat32 {
		return
	}

	obs := math.Float32bits(v)
	obs ^= uint32(int32(obs)>>31) | (1 << 31)

	l1i := (obs >> 27) & levelMask
	l1a := (*ptr)(ptr(&h.l0.l1[l1i]))
	l1 := (*level1)(atomic.LoadPointer(l1a))
	if l1 == nil {
		l1 = new(level1)
		if !atomic.CompareAndSwapPointer(l1a, nil, ptr(l1)) {
			l1 = (*level1)(atomic.LoadPointer(l1a))
		} else {
			h.l0.bm.Set(uint(l1i))
		}
	}

	l2i := (obs >> 22) & levelMask
	l2a := (*ptr)(ptr(&l1.l2[l2i]))
	l2 := (*level2)(atomic.LoadPointer(l2a))
	if l2 == nil {
		l2 = new(level2)
		if !atomic.CompareAndSwapPointer(l2a, nil, ptr(l2)) {
			l2 = (*level2)(atomic.LoadPointer(l2a))
		} else {
			l1.bm.Set(uint(l2i))
		}
	}

	atomic.AddUint64(&l2[(obs>>17)&levelMask], 1)
}

func (h *Histogram) Total() (total int64) {
	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := (*level1)(atomic.LoadPointer((*ptr)(ptr(&h.l0.l1[i]))))

		bm := l1.bm.Clone()
		for {
			i, ok := bm.Next()
			if !ok {
				break
			}
			l2 := (*level2)(atomic.LoadPointer((*ptr)(ptr(&l1.l2[i]))))

			for i := 0; i < levelSize; i++ {
				total += int64(atomic.LoadUint64(&l2[i]))
			}
		}
	}

	return total
}

func (h *Histogram) Quantile(q float64) float32 {
	target, acc := uint64(q*float64(h.Total())+0.5), uint64(0)

	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := (*level1)(atomic.LoadPointer((*ptr)(ptr(&h.l0.l1[i]))))

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := (*level2)(atomic.LoadPointer((*ptr)(ptr(&l1.l2[j]))))

			for k := uint32(0); k < levelSize; k++ {
				acc += atomic.LoadUint64(&l2[k])
				if acc >= target {
					obs := i<<27 | j<<22 | k<<17
					obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
					return math.Float32frombits(obs)
				}
			}
		}
	}

	return math.Float32frombits((1<<15 - 1) << 17)
}

func (h *Histogram) CDF(v float32) float64 {
	obs := math.Float32bits(v)
	obs ^= uint32(int32(obs)>>31) | (1 << 31)

	var sum, total uint64

	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := (*level1)(atomic.LoadPointer((*ptr)(ptr(&h.l0.l1[i]))))

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := (*level2)(atomic.LoadPointer((*ptr)(ptr(&l1.l2[j]))))

			target := i<<27 | j<<22

			for k := uint32(0); k < levelSize; k++ {
				count := atomic.LoadUint64(&l2[k])
				if obs >= target {
					sum += count
					target += 1 << 17
				}
				total += count
			}
		}
	}

	return float64(sum) / float64(total)
}

func (h *Histogram) Sum() (sum float64) {
	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := (*level1)(atomic.LoadPointer((*ptr)(ptr(&h.l0.l1[i]))))

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := (*level2)(atomic.LoadPointer((*ptr)(ptr(&l1.l2[j]))))

			for k := uint32(0); k < levelSize; k++ {
				count := float64(atomic.LoadUint64(&l2[k]))
				obs := i<<27 | j<<22 | k<<17 | 1<<16
				obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
				value := float64(math.Float32frombits(obs))

				sum += count * value
			}
		}
	}

	return sum
}

func (h *Histogram) Average() (sum, avg float64) {
	var total float64

	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := (*level1)(atomic.LoadPointer((*ptr)(ptr(&h.l0.l1[i]))))

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := (*level2)(atomic.LoadPointer((*ptr)(ptr(&l1.l2[j]))))

			for k := uint32(0); k < levelSize; k++ {
				count := float64(atomic.LoadUint64(&l2[k]))
				obs := i<<27 | j<<22 | k<<17 | 1<<16
				obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
				value := float64(math.Float32frombits(obs))

				total += count
				sum += count * value
			}
		}
	}

	if total == 0 {
		return 0, 0
	}
	return sum, sum / total
}

func (h *Histogram) Variance() (sum, avg, vari float64) {
	var total, total2 float64

	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := (*level1)(atomic.LoadPointer((*ptr)(ptr(&h.l0.l1[i]))))

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := (*level2)(atomic.LoadPointer((*ptr)(ptr(&l1.l2[j]))))

			for k := uint32(0); k < levelSize; k++ {
				count := float64(atomic.LoadUint64(&l2[k]))
				obs := i<<27 | j<<22 | k<<17 | 1<<16
				obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
				value := float64(math.Float32frombits(obs))

				total += count
				total2 += count * count
				avg_ := avg
				avg += (count / total) * (value - avg_)
				sum += count * value
				vari += count * (value - avg_) * (value - avg)
			}
		}
	}

	if total == 0 {
		return 0, 0, 0
	} else if total == 1 {
		return sum, sum / total, 0
	}
	return sum, sum / total, vari / (total - 1)
}
