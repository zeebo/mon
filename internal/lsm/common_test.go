package lsm

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/zeebo/assert"
)

func tempDir(tb testing.TB) (string, func()) {
	dir, err := ioutil.TempDir("", "lsm-")
	assert.NoError(tb, err)
	return dir, func() {
		assert.NoError(tb, os.RemoveAll(dir))
	}
}

func tempFile(tb testing.TB) (*os.File, func()) {
	fh, err := ioutil.TempFile("", "lsm-")
	assert.NoError(tb, err)
	return fh, func() {
		assert.NoError(tb, fh.Close())
		assert.NoError(tb, os.Remove(fh.Name()))
	}
}

func tempWriteHandle(tb testing.TB, cap int) (*writeHandle, func()) {
	fh, cleanup := tempFile(tb)
	wh, err := newWriteHandle(fh, cap)
	if err != nil {
		cleanup()
		assert.NoError(tb, err)
	}
	return wh, cleanup
}

func fileReset(tb testing.TB, fh *os.File) {
	fileSeekStart(tb, fh)
	assert.NoError(tb, fh.Truncate(0))
}

func fileSeekStart(tb testing.TB, fh *os.File) {
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

func fileContents(t *testing.T, fh *os.File) []byte {
	data, err := ioutil.ReadFile(fh.Name())
	assert.NoError(t, err)
	return data
}
