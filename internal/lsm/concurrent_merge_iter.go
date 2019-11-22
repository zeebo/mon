// +build ignore

package lsm

import (
	"sync"

	"github.com/zeebo/errs"
)

type concurrentMergeIter struct {
	mergeIter
	err  error
	ch   chan concurrentMergeIterItem
	mu   sync.Mutex
	stop chan struct{}
}

type concurrentMergeIterItem struct {
	ent entry
	err error
}

func newConcurrentMergeIter(it mergeIter, size int) *concurrentMergeIter {
	var cia concurrentMergeIter
	initConcurrentMergeIter(&cia, it, size)
	return &cia
}

func initConcurrentMergeIter(cia *concurrentMergeIter, it mergeIter, size int) {
	cia.mergeIter = it
	cia.ch = make(chan concurrentMergeIterItem, size)
	cia.stop = make(chan struct{})
	go cia.run()
}

func (cia *concurrentMergeIter) run() {
	defer close(cia.ch)

	for {
		ent, err := cia.mergeIter.Next()

		select {
		case cia.ch <- concurrentMergeIterItem{ent: ent, err: err}:
		case <-cia.stop:
			return
		}

		if err != nil {
			return
		}
	}
}

func (cia *concurrentMergeIter) Stop() {
	cia.mu.Lock()
	defer cia.mu.Unlock()

	select {
	case <-cia.stop:
	default:
		close(cia.stop)
	}
}

func (cia *concurrentMergeIter) Next() (entry, error) {
	if cia.err != nil {
		return entry{}, cia.err
	}
	item, ok := <-cia.ch
	if !ok {
		item.err = errs.New("iterator stopped")
	}
	cia.err = item.err
	return item.ent, item.err
}
