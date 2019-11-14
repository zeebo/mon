package lsm

import "github.com/zeebo/errs"

type batchMergeIter interface {
	inlinePtrReader

	ReadEntries(buf []entry) (int, error)
}

type batchMergeIterAdapter struct {
	batchMergeIter
	ents []entry
	idx  int
	err  error
}

func newBatchMergeIterAdapter(batch batchMergeIter, size int) *batchMergeIterAdapter {
	var mia batchMergeIterAdapter
	initBatchMergeIterAdapter(&mia, batch, size)
	return &mia
}

func initBatchMergeIterAdapter(mia *batchMergeIterAdapter, batch batchMergeIter, size int) {
	mia.batchMergeIter = batch
	mia.ents = make([]entry, size)
	mia.idx = size
}

func (mia *batchMergeIterAdapter) Next() (entry, error) {
	for {
		if mia.idx >= 0 && mia.idx < len(mia.ents) {
			ent := mia.ents[mia.idx]
			mia.idx++
			return ent, nil
		} else if mia.err != nil {
			mia.ents = nil
			return entry{}, mia.err
		}

		var n int
		n, mia.err = mia.batchMergeIter.ReadEntries(mia.ents[:cap(mia.ents)])
		if n >= 0 && n <= cap(mia.ents) {
			mia.ents = mia.ents[:n]
		} else {
			if mia.err == nil {
				mia.err = errs.New("invalid batch response: %d/%d", n, cap(mia.ents))
			}
			mia.ents = nil
		}
		mia.idx = 0
	}
}
