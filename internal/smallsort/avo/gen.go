package main

import (
	"sort"

	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	. "github.com/mmcloughlin/avo/reg"
)

func main() {
	TEXT("sort_sse3", NOSPLIT, "func(in *[8]uint64, out *[8]uint8)")

	in := Mem{Base: Load(Param("in"), GP64())}
	out := Mem{Base: Load(Param("out"), GP64())}

	_, _ = in, out

	loaded := [8]bool{}
	vals := [8]GPVirtual{
		GP64(), GP64(), GP64(), GP64(),
		GP64(), GP64(), GP64(), GP64(),
	}

	for i := range vals {
		MOVQ(in.Offset(8*i), vals[i])
		loaded[i] = true
	}

	var cs conds
	for b := 0; b < 7; b++ {
		for a := b + 1; a < 8; a++ {
			cs = append(cs, cond{a, b})
		}
	}
	sort.Sort(cs)

	tmp := [1]GPVirtual{}
	for i := range tmp {
		tmp[i] = GP64()
	}

	acc := [4]GPVirtual{}
	for i := range acc {
		acc[i] = GP64()
		XORQ(acc[i], acc[i])
	}

	for i, c := range cs {
		if !loaded[c.a] {
			MOVQ(in.Offset(8*c.a), vals[c.a])
			loaded[c.a] = true
		}
		if !loaded[c.b] {
			MOVQ(in.Offset(8*c.b), vals[c.b])
			loaded[c.b] = true
		}

		if val := c.val(); val < 1<<31 {
			LEAQ(Mem{Base: acc[i%len(acc)]}.Offset(c.val()), tmp[i%len(tmp)])
		} else {
			MOVQ(U64(val), tmp[i%len(tmp)])
			ADDQ(acc[i%len(acc)], tmp[i%len(tmp)])
		}

		CMPQ(vals[c.a], vals[c.b])
		CMOVQCS(tmp[i%len(tmp)], acc[i%len(acc)])
	}

	// vbuf, vout := X1, X0
	// MOVQ(buf, vbuf)

	// imm := GP64()
	// MOVQ(Imm(0x0706050403020100), imm)
	// MOVQ(imm, vout)

	// PSHUFB(vout, vbuf)
	// MOVQ(vout, out.Offset(0))

	for i := range acc[1:] {
		ADDQ(acc[i+1], acc[0])
	}
	MOVQ(acc[0], out.Offset(0))

	RET()
	Generate()
}

type cond struct{ a, b int }

func (c *cond) val() (v int) {
	const bits = 8
	v = 1 << (bits * (c.a - 1))
	if c.b > 0 {
		v -= 1 << (bits * (c.b - 1))
	}
	return v
}

type conds []cond

func (c conds) Len() int           { return len(c) }
func (c conds) Less(i, j int) bool { return c[i].val() < c[j].val() }
func (c conds) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
