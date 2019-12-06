package waliter

import (
	"github.com/zeebo/mon/internal/lsm/buffer"
	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/file"
)

type T struct {
	buf      buffer.T
	consumed int64
	prefix   bool
}

func New(fh file.T) *T {
	var wi T
	wi.Init(fh)
	return &wi
}

func (wi *T) Init(fh file.T) {
	wi.buf.Init(fh, 4096)
}

func (w *T) Consumed() (int64, bool) { return w.consumed, w.prefix }

func (w *T) Err() error { return w.buf.Err() }

func (w *T) Next() (ent entry.T, key, value []byte, ok bool) {
	consumed := int64(entry.Size)
	data, ok := w.buf.Read(int(entry.Size))
	if !ok {
		goto bad
	}
	copy(ent[:], data)

	if ent.Key().Pointer() {
		key, ok = w.buf.Read(ent.Key().Length())
		if !ok {
			goto bad
		}
		consumed += int64(len(key))
	} else if ent.Key().Inline() {
		key = ent.Key().InlineData()
	}

	if ent.Value().Pointer() {
		value, ok = w.buf.Read(ent.Value().Length())
		if !ok {
			goto bad
		}
		consumed += int64(len(value))
	} else if ent.Value().Inline() {
		value = ent.Value().InlineData()
	}

	w.consumed += consumed
	return ent, key, value, true

bad:
	w.prefix = w.buf.Buffered() > 0
	return entry.T{}, nil, nil, false
}
