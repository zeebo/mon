package lsm

import "bytes"

type entryHeapElement struct {
	ent  entry
	mkey []byte // maybe key
	idx  int
}

func (e *entryHeapElement) Key() []byte {
	if e.mkey != nil {
		return e.mkey
	}
	return e.ent.Key().InlineData()
}

func entryHeapElementLess(i, j *entryHeapElement) bool {
	if ip, jp := i.ent.Key().Prefix(), j.ent.Key().Prefix(); ip < jp {
		return true
	} else if ip > jp {
		return false
	}

	ik := i.mkey
	if ik == nil {
		ik = i.ent.Key().InlineData()
	}
	jk := j.mkey
	if jk == nil {
		jk = j.ent.Key().InlineData()
	}

	if cmp := bytes.Compare(ik, jk); cmp == -1 {
		return true
	} else if cmp == 1 {
		return false
	}

	return i.idx < j.idx
}

type entryHeap struct {
	eles []entryHeapElement
}

func newEntryHeap(eles []entryHeapElement) *entryHeap {
	var eh entryHeap
	initEntryHeap(&eh, eles)
	return &eh
}

func initEntryHeap(eh *entryHeap, eles []entryHeapElement) {
	for i := len(eles)/2 - 1; i >= 0; i-- {
		entryHeapDown(eles, i)
	}
	eh.eles = eles
}

func (eh *entryHeap) Push(ele entryHeapElement) {
	eh.eles = append(eh.eles, ele)
	entryHeapUp(eh.eles)
}

func (eh *entryHeap) Pop() (ele entryHeapElement, ok bool) {
	if eles := eh.eles; len(eles) > 0 {
		n := len(eles) - 1

		ele, ok = eles[0], true
		eles[0] = eles[n]
		entryHeapDown(eles, 0)
		eh.eles = eles[:n]
	}

	return ele, ok
}

func entryHeapUp(eles []entryHeapElement) {
	i := len(eles) - 1
	if i < 0 || i >= len(eles) {
		return
	}
	elei := &eles[i]

next:
	j := (i - 1) / 2
	if i != j && j >= 0 && j < len(eles) {
		elej := &eles[j]
		if entryHeapElementLess(elei, elej) {
			*elei, *elej = *elej, *elei
			elei, i = elej, j
			goto next
		}
	}
}

func entryHeapDown(eles []entryHeapElement, i int) {
	if i < 0 || i >= len(eles) {
		return
	}
	elei := &eles[i]

next:
	j1 := 2*i + 1
	if j1 >= 0 && j1 < len(eles) {
		elej, j := &eles[j1], j1

		if j2 := j1 + 1; j2 >= 0 && j2 < len(eles) {
			if entryHeapElementLess(&eles[j2], &eles[j1]) {
				elej, j = &eles[j2], j2
			}
		}

		if entryHeapElementLess(elej, elei) {
			*elei, *elej = *elej, *elei
			elei, i = elej, j
			goto next
		}
	}
}
