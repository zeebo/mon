package lsm

import (
	"fmt"
	"testing"

	"github.com/zeebo/pcg"
)

func TestBtree(t *testing.T) {
	var bt btree

	var buf []byte
	var vptrs []inlinePtr

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprint(pcg.Uint32()))
		value := []byte(fmt.Sprint(pcg.Uint32()))
		kptr := newInlinePtrBytes(key)
		vptr := newInlinePtrBytes(value)

		if kptr.Pointer() {
			kptr.SetOffset(uint64(len(buf)))
			buf = append(buf, key...)
		}
		if vptr.Pointer() {
			vptr.SetOffset(uint64(len(buf)))
			buf = append(buf, value...)
		}

		bt.Insert(kptr, uint32(i), key, buf)
		vptrs = append(vptrs, vptr)
	}

	iter := bt.Iterator(buf, vptrs)
	for iter.Advance() {
		// fmt.Println(string(iter.Entry().Key().InlineData()))
	}
}
