package lsm

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/zeebo/assert"
)

var nullFile, _ = os.OpenFile(os.DevNull, os.O_RDWR|os.O_APPEND, 0644)

func tempFile(t *testing.T) (*os.File, func()) {
	fh, err := ioutil.TempFile("", "lsm-")
	assert.NoError(t, err)
	return fh, func() {
		assert.NoError(t, fh.Close())
		assert.NoError(t, os.Remove(fh.Name()))
	}
}

func fileReset(t *testing.T, fh *os.File) {
	_, err := fh.Seek(0, io.SeekStart)
	assert.NoError(t, err)
}

func fileContents(t *testing.T, fh *os.File) []byte {
	data, err := ioutil.ReadFile(fh.Name())
	assert.NoError(t, err)
	return data
}
