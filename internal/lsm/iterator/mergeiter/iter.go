package mergeiter

import (
	"bytes"
	"math/bits"

	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/iterator"
)

type T struct {
	iters []iterator.T
	trn   []int
	win   int
	first bool
	last  []byte
	err   error
}

func New(iters []iterator.T) *T {
	var m T
	m.Init(iters)
	return &m
}

func (m *T) Init(iters []iterator.T) {
	leaves := 1 << uint(bits.Len(uint(len(iters)-1)))
	trn := make([]int, leaves-1)
	wins := make([]int, 2*leaves-1)

	for i := range wins {
		wins[i] = i

		if uint(i) < uint(len(iters)) {
			if iter := iters[i]; !iter.Next() {
				if m.err = iter.Err(); m.err != nil {
					return
				}
				iters[i] = nil
			}
		}
	}

	for i := range trn {
		l, r := wins[2*i], wins[2*i+1]

		if uint(l) >= uint(len(iters)) || iters[l] == nil {
			goto noSwap
		} else if uint(r) >= uint(len(iters)) || iters[r] == nil {
			// swap
		} else if cmp := bytes.Compare(iters[l].Key(), iters[r].Key()); cmp < 0 || (cmp == 0 && l < r) {
			// swap
		} else {
			goto noSwap
		}

		r, l = l, r

	noSwap:
		trn[i] = l
		wins[leaves+i] = r
	}

	m.iters = iters
	m.trn = trn
	m.win = wins[len(wins)-1]

	if uint(m.win) < uint(len(iters)) && iters[m.win] != nil {
		m.last = append(m.last, iters[m.win].Key()...)
	}
}

func (m *T) Err() error { return m.err }

func (m *T) Entry() (e entry.T) {
	if uint(m.win) < uint(len(m.iters)) {
		e = m.iters[m.win].Entry()
	}
	return
}

func (m *T) Key() (k []byte) {
	if uint(m.win) < uint(len(m.iters)) {
		k = m.iters[m.win].Key()
	}
	return
}

func (m *T) Value() (v []byte) {
	if uint(m.win) < uint(len(m.iters)) {
		v = m.iters[m.win].Value()
	}
	return
}

func (m *T) Next() bool {
	if m.err != nil {
		return false
	}

	iters, trn, win := m.iters, m.trn, m.win

again:
	if uint(win) >= uint(len(iters)) || iters[win] == nil {
		return false
	}

	if !m.first {
		m.first = true
		return true
	}

	var wkey []byte

	if iter := iters[win]; !iter.Next() {
		if m.err = iter.Err(); m.err != nil {
			return false
		}
		iters[win] = nil
	} else {
		wkey = iter.Key()
	}

	offset := (len(trn) + 1) / 2
	for idx := win / 2; uint(idx) < uint(len(trn)); idx = offset + idx/2 {
		var ckey []byte
		chal := trn[idx]

		if uint(chal) >= uint(len(iters)) || iters[chal] == nil {
			goto noSwap
		} else if ckey = iters[chal].Key(); uint(win) >= uint(len(iters)) || iters[win] == nil {
			// swap
		} else if cmp := bytes.Compare(ckey, wkey); cmp == -1 || (cmp == 0 && chal < win) {
			// swap
		} else {
			goto noSwap
		}

		trn[idx], win, wkey = win, chal, ckey

	noSwap:
	}

	m.win = win

	if uint(win) >= uint(len(iters)) || iters[win] == nil {
		return false
	}

	if bytes.Equal(m.last, wkey) {
		goto again
	}
	m.last = append(m.last[:0], wkey...)

	return true
}
