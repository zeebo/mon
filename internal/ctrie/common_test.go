package ctrie

import (
	"fmt"
	"sync"
	"unsafe"
)

var (
	cacheMu sync.Mutex
	ptrMap  = make([]unsafe.Pointer, 10000)
	keyMap  = make([]string, 10000)
)

func init() {
	for i := 0; i < 10000; i++ {
		ptrMap[i] = unsafe.Pointer(new(int))
		keyMap[i] = fmt.Sprint(i)
	}
}

func iptr(i int) (p unsafe.Pointer) {
	return ptrMap[i]
}

func ikey(i int) (s string) {
	return keyMap[i]
}
