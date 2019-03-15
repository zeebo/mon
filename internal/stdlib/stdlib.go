package stdlib

import (
	"sync"
	"unsafe"
)

type T struct {
	mu sync.RWMutex
	m  map[string]unsafe.Pointer
}

func New() *T {
	return &T{m: make(map[string]unsafe.Pointer)}
}

func (t *T) Upsert(k string, vf func() unsafe.Pointer) unsafe.Pointer {
	t.mu.RLock()
	v, ok := t.m[k]
	t.mu.RUnlock()

	if ok {
		return v
	}

	t.mu.Lock()
	v, ok = t.m[k]
	if !ok {
		v = vf()
		t.m[k] = v
	}
	t.mu.Unlock()

	return v
}

func (t *T) Lookup(k string) unsafe.Pointer {
	t.mu.RLock()
	v := t.m[k]
	t.mu.RUnlock()
	return v
}
