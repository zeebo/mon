package heapmem

import "github.com/zeebo/mon/internal/lsm/entry"

func heapUp(data []byte, ptrs []entry.T) {
	i := len(ptrs) - 1
	if i < 0 || i >= len(ptrs) {
		return
	}
	ptri := ptrs[i].Key()
	ip := ptri.Prefix()

next:
	j := (i - 1) / 2
	if i != j && j >= 0 && j < len(ptrs) {
		ptrj := ptrs[j].Key()
		jp := ptrj.Prefix()

		if ip > jp {
			return
		} else if ip == jp {
			var ki []byte
			if ptri.Pointer() {
				begin := ptri.Offset()
				end := begin + uint64(ptri.Length())
				ki = data[begin:end]
			} else {
				ki = ptri.InlineData()
			}

			var kj []byte
			if ptrj.Pointer() {
				begin := ptrj.Offset()
				end := begin + uint64(ptrj.Length())
				kj = data[begin:end]
			} else {
				kj = ptrj.InlineData()
			}

			if string(ki) >= string(kj) {
				return
			}
		}

		*ptri, *ptrj = *ptrj, *ptri
		ptri, i, ip = ptrj, j, jp
		goto next
	}
}

func heapDown(data []byte, ptrs []entry.T) {
	if len(ptrs) == 0 {
		return
	}
	ptri, i := ptrs[0].Key(), 0
	ip := ptri.Prefix()

next:
	j1 := 2*i + 1
	if j1 >= 0 && j1 < len(ptrs) {
		ptrj, j := ptrs[j1].Key(), j1
		jp := ptrj.Prefix()

		if j2 := j1 + 1; j2 >= 0 && j2 < len(ptrs) {
			ptrj2 := ptrs[j2].Key()
			jp2 := ptrj2.Prefix()

			if jp2 < jp {
				ptrj, j, jp = ptrj2, j2, jp2
			} else if jp2 == jp {
				var kj []byte
				if ptrj.Pointer() {
					begin := ptrj.Offset()
					end := begin + uint64(ptrj.Length())
					kj = data[begin:end]
				} else {
					kj = ptrj.InlineData()
				}

				var kj2 []byte
				if ptrj2.Pointer() {
					begin := ptrj2.Offset()
					end := begin + uint64(ptrj2.Length())
					kj2 = data[begin:end]
				} else {
					kj2 = ptrj2.InlineData()
				}

				if string(kj2) < string(kj) {
					ptrj, j, jp = ptrj2, j2, jp2
				}
			}
		}

		if ip > jp {
			return
		} else if ip == jp {
			var ki []byte
			if ptri.Pointer() {
				begin := ptri.Offset()
				end := begin + uint64(ptri.Length())
				ki = data[begin:end]
			} else {
				ki = ptri.InlineData()
			}

			var kj []byte
			if ptrj.Pointer() {
				begin := ptrj.Offset()
				end := begin + uint64(ptrj.Length())
				kj = data[begin:end]
			} else {
				kj = ptrj.InlineData()
			}

			if string(ki) >= string(kj) {
				return
			}
		}

		*ptri, *ptrj = *ptrj, *ptri
		ptri, i, ip = ptrj, j, jp
		goto next
	}
}
