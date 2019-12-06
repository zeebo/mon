package testutil

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/lsm/file"
)

func TempDir(tb testing.TB) (string, func()) {
	dir, err := ioutil.TempDir("", "lsm-")
	assert.NoError(tb, err)
	return dir, func() {
		assert.NoError(tb, os.RemoveAll(dir))
	}
}

func TempFile(tb testing.TB) (file.T, func()) {
	tmpdir := os.Getenv("TMPDIR")
	if tmpdir == "" {
		tmpdir = "/tmp"
	}
	name := filepath.Join(tmpdir, fmt.Sprint(time.Now().UnixNano())+"\x00")

	fh, err := file.Create(name)
	assert.NoError(tb, err)
	return fh, func() {
		assert.NoError(tb, fh.Close())
		assert.NoError(tb, file.Remove(name))
	}
}

func ResetFile(tb testing.TB, fh file.T) {
	SeekStartFile(tb, fh)
	assert.NoError(tb, fh.Truncate(0))
}

func SeekStartFile(tb testing.TB, fh file.T) {
	_, err := fh.Seek(0, io.SeekStart)
	assert.NoError(tb, err)
}
