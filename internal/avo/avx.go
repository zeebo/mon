// +build gen

package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	. "github.com/mmcloughlin/avo/reg"
)

func main() {
	{
		TEXT("sum_histogram64", NOSPLIT, "func(data *[64]uint64) uint64")
		data := Mem{Base: Load(Param("data"), RAX)}

		y0 := YMM()
		x0 := y0.AsX()

		VMOVDQU(data.Offset(0), y0)
		for i := 32; i < 8*64; i += 32 {
			VPADDQ(data.Offset(i), y0, y0)
		}

		a, b, c, d := GP64(), GP64(), GP64(), GP64()

		VMOVQ(x0, a)
		VPEXTRQ(Imm(1), x0, b)
		VEXTRACTI128(Imm(1), y0, x0)
		VMOVQ(x0, c)
		VPEXTRQ(Imm(1), x0, d)

		ADDQ(b, a)
		ADDQ(c, a)
		ADDQ(d, a)

		Store(a, ReturnIndex(0))
		RET()
	}

	{
		TEXT("sum_histogram32", NOSPLIT, "func(data *[64]uint32) uint64")
		data := Mem{Base: Load(Param("data"), RAX)}

		y0, y1 := YMM(), YMM()
		x0, x1 := y0.AsX(), y1.AsX()

		VPMOVZXDQ(data.Offset(0), y0)
		for i := 16; i <= 240; i += 16 {
			VPMOVZXDQ(data.Offset(i), y1)
			VPADDQ(y0, y1, y0)
		}

		VEXTRACTI128(Imm(1), y0, x1)
		VPADDQ(y0, y1, y0)
		VPSHUFD(Imm(0b01_00_11_10), x0, x1)
		VPADDQ(x0, x1, x0)
		VMOVQ(x0, RBX)

		Store(RBX, ReturnIndex(0))
		RET()
	}

	Generate()
}
