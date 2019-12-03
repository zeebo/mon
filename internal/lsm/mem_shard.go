package lsm

import (
	"github.com/zeebo/xxh3"
	"runtime"
	"sync"
	"unsafe"
)

type mem = heapMem

type shardMem struct {
	cap  uint64
	mus  []sync.Mutex
	mems []mem
}

func (m *shardMem) init(cap uint64) {
	cpus := uint64(runtime.NumCPU())
	m.mus = make([]sync.Mutex, cpus)
	m.mems = make([]mem, cpus)

	m.cap = cap
	for i := range m.mems {
		m.mems[i].init(cap / cpus)
	}
}

func (m *shardMem) reset() {
	for i := range m.mems {
		m.mems[i].reset()
	}
}

func (m *shardMem) Keys() uint32 {
	keys := uint32(0)
	for i := range m.mems {
		keys += m.mems[i].Keys()
	}
	return keys
}

func (m *shardMem) Cap() uint64 { return m.cap }

func (m *shardMem) Len() uint64 {
	len := uint64(0)
	for i := range m.mems {
		len += m.mems[i].Len()
	}
	return len
}

func (m *shardMem) SetBytes(key, value []byte) bool {
	return m.SetString(*(*string)(unsafe.Pointer(&key)), value)
}

func (m *shardMem) SetString(key string, value []byte) bool {
	shard := xxh3.HashString(key) % uint64(len(m.mus))
	mu := &m.mus[shard]

	mu.Lock()
	ok := m.mems[shard].SetString(key, value)
	mu.Unlock()

	return ok
}

func (m *shardMem) iters() (is []iterator) {
	for i := range m.mems {
		it := m.mems[i].iter()
		is = append(is, &it)
	}
	return is
}
