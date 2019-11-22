// Copyright (C) 2018. See AUTHORS.

package system

import (
	"io"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

type File uintptr

func Create(path string) (f File, err error) {
	fd, _, ec := syscall.Syscall(
		syscall.SYS_OPEN,
		uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&path))),
		uintptr(os.O_RDWR|os.O_CREATE),
		0644)
	if ec != 0 {
		return 0, ec
	}

	runtime.KeepAlive(path)
	return File(fd), nil
}

func Open(path string) (f File, err error) {
	fd, _, ec := syscall.Syscall(
		syscall.SYS_OPEN,
		uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&path))),
		uintptr(os.O_RDONLY),
		0644)
	if ec != 0 {
		return 0, ec
	}

	runtime.KeepAlive(path)
	return File(fd), nil
}

func (f File) Close() (err error) {
	return syscall.Close(int(f))
}

func (f File) Write(p []byte) (n int, err error) {
	n, err = syscall.Write(int(f), p)
	return n, err
}

func (f File) Read(p []byte) (n int, err error) {
	n, err = syscall.Read(int(f), p)
	if err == nil && n == 0 {
		return 0, io.EOF
	}
	return n, err
}

func (f File) Seek(offset int64, whence int) (off int64, err error) {
	off, err = syscall.Seek(int(f), offset, whence)
	return off, err
}

func (f File) Truncate(n int64) (err error) {
	return syscall.Ftruncate(int(f), n)
}

func (f File) Sync() (err error) {
	return syscall.Fsync(int(f))
}

func (f File) Fd() int { return int(f) }

var _AT_FDCWD = -0x64

func Rename(old, new string) error {
	_, _, ec := syscall.Syscall6(syscall.SYS_RENAMEAT,
		uintptr(_AT_FDCWD),
		uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&old))),
		uintptr(_AT_FDCWD),
		uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&new))),
		0, 0)
	if ec != 0 {
		return ec
	}

	runtime.KeepAlive(old)
	runtime.KeepAlive(new)
	return nil
}

func Remove(path string) error {
	_, _, ec := syscall.Syscall(syscall.SYS_UNLINKAT,
		uintptr(_AT_FDCWD),
		uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&path))),
		0)

	if ec != 0 {
		return ec
	}

	runtime.KeepAlive(path)
	return nil
}
