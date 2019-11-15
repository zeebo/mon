package lsm

import (
	"fmt"
	"io"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/errs"
)

func TestBatchMergeIterAdapter(t *testing.T) {
	mk := func(key string) entry { return newEntry(newInlinePtrString(key), inlinePtr{}) }

	t.Run("Basic", func(t *testing.T) {
		var bi fakeBatchMergeIter
		for i := 0; i < 1000; i++ {
			bi = append(bi, mk(fmt.Sprint(i)))
		}

		iter := newBatchMergeIterAdapter(&bi, 4096/32)
		for i := 0; i < 1000; i++ {
			ent, err := iter.Next()
			assert.NoError(t, err)

			var key []byte
			if kptr := ent.Key(); kptr.Pointer() {
				key, err = iter.ReadPointer(*kptr)
				assert.NoError(t, err)
			} else if kptr.Inline() {
				key = kptr.InlineData()
			}

			assert.Equal(t, string(key), fmt.Sprint(i))
		}
	})
}

type fakeBatchMergeIter []entry

func (f *fakeBatchMergeIter) ReadPointer(ptr inlinePtr) ([]byte, error) {
	return nil, errs.New("unimplemented")
}

func (f *fakeBatchMergeIter) ReadEntries(buf []entry) (int, error) {
	if len(*f) == 0 {
		return 0, io.EOF
	}
	n := copy(buf, *f)
	*f = (*f)[n:]
	return n, nil
}
