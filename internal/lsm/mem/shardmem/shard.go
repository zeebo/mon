package shardmem

import (
	"runtime"
	"sync"
	"unsafe"

	"github.com/zeebo/mon/internal/lsm/iterator"
	"github.com/zeebo/mon/internal/lsm/mem"
	"github.com/zeebo/xxh3"
)

type T struct {
	cap  uint64
	mus  []sync.Mutex
	mems []mem.T
}

func (m *T) Init(cap uint64) {
	cpus := uint64(runtime.NumCPU())
	m.mus = make([]sync.Mutex, cpus)
	m.mems = make([]mem.T, cpus)

	m.cap = cap
	for i := range m.mems {
		m.mems[i].Init(cap / cpus)
	}
}

func (m *T) Reset() {
	for i := range m.mems {
		m.mems[i].Reset()
	}
}

func (m *T) Keys() uint32 {
	keys := uint32(0)
	for i := range m.mems {
		keys += m.mems[i].Keys()
	}
	return keys
}

func (m *T) Cap() uint64 { return m.cap }

func (m *T) Len() uint64 {
	len := uint64(0)
	for i := range m.mems {
		len += m.mems[i].Len()
	}
	return len
}

func (m *T) SetBytes(key, value []byte) bool {
	return m.SetString(*(*string)(unsafe.Pointer(&key)), value)
}

func (m *T) SetString(key string, value []byte) bool {
	shard := xxh3.HashString(key) % uint64(len(m.mus))
	mu := &m.mus[shard]

	mu.Lock()
	ok := m.mems[shard].SetString(key, value)
	mu.Unlock()

	return ok
}

func (m *T) Iters() (is []iterator.T) {
	for i := range m.mems {
		it := m.mems[i].Iter()
		is = append(is, &it)
	}
	return is
}
