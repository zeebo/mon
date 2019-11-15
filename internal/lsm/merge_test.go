package lsm

import (
	"io"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/errs"
)

func TestMergedIterator(t *testing.T) {
	mk := func(key string) entry { return newEntry(newInlinePtrString(key), inlinePtr{}) }
	mks := func(keys ...string) (out *fakeMergeIterator) {
		out = new(fakeMergeIterator)
		for _, key := range keys {
			*out = append(*out, mk(key))
		}
		return out
	}

	t.Run("Basic", func(t *testing.T) {
		expect := "059abcdrst"
		mi, err := newMerger([]mergeIter{
			mks("a", "b", "c", "d"),
			mks("r", "s", "t"),
			mks(),
			mks("0", "5", "9"),
			mks("5", "a", "t"),
		})
		assert.NoError(t, err)

		for i := 0; i < len(expect); i++ {
			ele, _, err := mi.Next()
			assert.NoError(t, err)
			assert.Equal(t, string(ele.Key()), expect[i:i+1])
		}

		_, _, err = mi.Next()
		assert.Equal(t, err, io.EOF)
	})
}

type fakeMergeIterator []entry

func (f *fakeMergeIterator) ReadPointer(ptr inlinePtr) ([]byte, error) {
	return nil, errs.New("unimplemented")
}

func (f *fakeMergeIterator) Next() (entry, error) {
	if len(*f) == 0 {
		return entry{}, io.EOF
	}
	ent := (*f)[0]
	*f = (*f)[1:]
	return ent, nil
}
