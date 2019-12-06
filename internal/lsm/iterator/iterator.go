package iterator

import "github.com/zeebo/mon/internal/lsm/entry"

type T interface {
	Next() bool

	Entry() entry.T

	Key() []byte
	Value() []byte // Key is no longer valid after a call to Value until Next

	Err() error
}
