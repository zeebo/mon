package lsm

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/zeebo/assert"
)

func init() {
	nullFile, _ = os.OpenFile(os.DevNull, os.O_RDWR|os.O_APPEND, 0644)
}

func tempFile(tb testing.TB) (*os.File, func()) {
	fh, err := ioutil.TempFile("", "lsm-")
	assert.NoError(tb, err)
	return fh, func() {
		assert.NoError(tb, fh.Close())
		assert.NoError(tb, os.Remove(fh.Name()))
	}
}

func tempHandle(tb testing.TB, cap int) (*handle, func()) {
	fh, cleanup := tempFile(tb)
	handle, err := newHandle(fh, cap)
	if err != nil {
		cleanup()
		assert.NoError(tb, err)
	}
	return handle, cleanup
}

func fileReset(tb testing.TB, fh *os.File) {
	fileSeekStart(tb, fh)
	assert.NoError(tb, fh.Truncate(0))
}

func fileSeekStart(tb testing.TB, fh *os.File) {
	_, err := fh.Seek(0, io.SeekStart)
	assert.NoError(tb, err)
}

func handleReset(tb testing.TB, h *handle) {
	fileReset(tb, h.fh)
	h.off = 0
	h.buf = h.buf[:0]
}

func handleSeekStart(tb testing.TB, h *handle) {
	fileSeekStart(tb, h.fh)
	h.off = 0
	h.buf = h.buf[:0]
}

func fileContents(t *testing.T, fh *os.File) []byte {
	data, err := ioutil.ReadFile(fh.Name())
	assert.NoError(t, err)
	return data
}
