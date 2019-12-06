package fileiter

import (
	"github.com/zeebo/mon/internal/lsm/buffer"
	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/file"
	"github.com/zeebo/mon/internal/lsm/inlineptr"
)

type T struct {
	err       error
	entBuffer buffer.T
	valBuffer buffer.T
	ent       entry.T
	key       []byte
	val       []byte
}

func New(entries, values file.T) *T {
	var fi T
	fi.Init(entries, values)
	return &fi
}

func (fi *T) Init(entries, values file.T) {
	fi.entBuffer.Init(entries, buffer.Size)
	fi.valBuffer.Init(values, buffer.Size)
}

func (fi *T) Next() bool {
	if fi.err != nil {
		return false
	}

	buf, ok := fi.entBuffer.Read(entry.Size)
	if !ok {
		fi.err = fi.entBuffer.Err()
		return false
	}
	copy(fi.ent[:], buf)

	switch kptr := fi.ent.Key(); kptr[0] {
	case inlineptr.Inline:
		fi.key = append(fi.key[:0], kptr.InlineData()...)
	case inlineptr.Pointer:
		key, ok := fi.valBuffer.Read(kptr.Length())
		if !ok {
			fi.err = fi.valBuffer.Err()
			return false
		}
		fi.key = append(fi.key[:0], key...)
	}

	switch vptr := fi.ent.Value(); vptr[0] {
	case inlineptr.Inline:
		fi.val = append(fi.val[:0], vptr.InlineData()...)
	case inlineptr.Pointer:
		val, ok := fi.valBuffer.Read(vptr.Length())
		if !ok {
			fi.err = fi.valBuffer.Err()
			return false
		}
		fi.val = append(fi.val[:0], val...)
	}

	return true
}

func (fi *T) Entry() entry.T { return fi.ent }

func (fi *T) Key() []byte {
	if fi.ent.Key().Null() {
		return nil
	}
	return fi.key
}

func (fi *T) Value() []byte {
	if fi.ent.Value().Null() {
		return nil
	}
	return fi.val
}

func (fi *T) Err() error { return fi.err }
