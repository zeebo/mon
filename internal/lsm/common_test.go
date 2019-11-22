package lsm

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func makeEntry(key, value string) entry {
	return newEntry(newInlinePtrString(key), newInlinePtrString(value))
}

func randomEntry() entry {
	return makeEntry(fmt.Sprint(pcg.Uint32()), fmt.Sprint(pcg.Uint32()))
}

type fakeMergeIterator []entry

func (f *fakeMergeIterator) Err() error    { return nil }
func (f *fakeMergeIterator) Entry() entry  { return (*f)[0] }
func (f *fakeMergeIterator) Key() []byte   { return (*f)[0].Key().InlineData() }
func (f *fakeMergeIterator) Value() []byte { return (*f)[0].Value().InlineData() }

func (f *fakeMergeIterator) Next() bool {
	if len(*f) > 0 {
		*f = (*f)[1:]
	}
	return len(*f) > 0
}

func newFakeIter(value string, keys ...string) *fakeMergeIterator {
	out := fakeMergeIterator{entry{}}
	for _, key := range keys {
		out = append(out, makeEntry(key, value))
	}
	return &out
}

func newFakeIterRandom(count int) *fakeMergeIterator {
	out := fakeMergeIterator{entry{}}
	for i := 0; i < count; i++ {
		out = append(out, randomEntry())
	}
	sortEntries([]entry(out[1:]))
	return &out
}

func sortEntries(ents []entry) {
	sort.Slice(ents, func(i, j int) bool {
		return bytes.Compare(ents[i].Key().InlineData(), ents[j].Key().InlineData()) < 0
	})
}

//
//
//

func tempDir(tb testing.TB) (string, func()) {
	dir, err := ioutil.TempDir("", "lsm-")
	assert.NoError(tb, err)
	return dir, func() {
		assert.NoError(tb, os.RemoveAll(dir))
	}
}

func tempFile(tb testing.TB) (file, func()) {
	tmpdir := os.Getenv("TMPDIR")
	if tmpdir == "" {
		tmpdir = "/tmp"
	}
	name := filepath.Join(tmpdir, fmt.Sprint(time.Now().UnixNano())+"\x00")

	fh, err := fileCreate(name)
	assert.NoError(tb, err)
	return fh, func() {
		assert.NoError(tb, fh.Close())
		assert.NoError(tb, fileRemove(name))
	}
}

func tempWriteHandle(tb testing.TB, cap int) (*writeHandle, func()) {
	fh, cleanup := tempFile(tb)
	return newWriteHandle(fh, cap), cleanup
}

func fileReset(tb testing.TB, fh file) {
	fileSeekStart(tb, fh)
	assert.NoError(tb, fh.Truncate(0))
}

func fileSeekStart(tb testing.TB, fh file) {
	_, err := fh.Seek(0, io.SeekStart)
	assert.NoError(tb, err)
}

func writeHandleReset(tb testing.TB, wh *writeHandle) {
	fileReset(tb, wh.fh)
	wh.off = 0
	wh.buf = wh.buf[:0]
}

func writeHandleSeekStart(tb testing.TB, wh *writeHandle) {
	fileSeekStart(tb, wh.fh)
	wh.off = 0
	wh.buf = wh.buf[:0]
}
