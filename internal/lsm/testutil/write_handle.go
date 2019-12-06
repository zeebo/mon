package testutil

import (
	"testing"

	"github.com/zeebo/mon/internal/lsm/file/writehandle"
)

func TempWriteHandle(tb testing.TB, cap int) (*writehandle.T, func()) {
	fh, cleanup := TempFile(tb)
	return writehandle.New(fh, cap), cleanup
}

func ResetWriteHandle(tb testing.TB, wh *writehandle.T) {
	ResetFile(tb, wh.File())
	wh.Reset()
}

func SeekStartWriteHandle(tb testing.TB, wh *writehandle.T) {
	SeekStartFile(tb, wh.File())
	wh.Reset()
}
