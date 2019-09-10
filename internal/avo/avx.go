// +build gen

package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	. "github.com/mmcloughlin/avo/reg"
)

func main() {
	TEXT("sum_histogram", NOSPLIT, "func(data *[64]uint32) uint64")
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

	Generate()
}
