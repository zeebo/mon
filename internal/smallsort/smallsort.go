package smallsort

func Min(in *[8]uint64) (out uint8) {
	val := in[0]
	idx := uint64(0)

	if in[1] < val {
		idx, val = 1, in[1]
	}
	if in[2] < val {
		idx, val = 2, in[2]
	}
	if in[3] < val {
		idx, val = 3, in[3]
	}
	if in[4] < val {
		idx, val = 4, in[4]
	}
	if in[5] < val {
		idx, val = 5, in[5]
	}
	if in[6] < val {
		idx, val = 6, in[6]
	}
	if in[7] < val {
		idx = 7
	}

	return byte(idx)
}

func Min2(in *[8]uint64) (a, b uint8) {
	av, bv := in[0], in[1]
	ai, bi := uint64(0), uint64(1)
	if bv < av {
		bi, bv, ai, av = ai, av, bi, bv
	}

	if in[2] < av {
		bi, bv, ai, av = ai, av, 2, in[2]
	} else if in[2] < bv {
		bi, bv = 2, in[2]
	}
	if in[3] < av {
		bi, bv, ai, av = ai, av, 3, in[3]
	} else if in[3] < bv {
		bi, bv = 3, in[3]
	}
	if in[4] < av {
		bi, bv, ai, av = ai, av, 4, in[4]
	} else if in[4] < bv {
		bi, bv = 4, in[4]
	}
	if in[5] < av {
		bi, bv, ai, av = ai, av, 5, in[5]
	} else if in[5] < bv {
		bi, bv = 5, in[5]
	}
	if in[6] < av {
		bi, bv, ai, av = ai, av, 6, in[6]
	} else if in[6] < bv {
		bi, bv = 6, in[6]
	}
	if in[7] < av {
		bi, ai = ai, 7
	} else if in[7] < bv {
		bi = 7
	}

	return byte(ai), byte(bi)
}

func Sort(in *[8]uint64) (out [8]uint8) {
	var sa, sb, sc, sd uint32

	if in[1] < in[0] {
		sa += 1 << 0
	}
	if in[2] < in[1] {
		sb += 1<<3 - 1<<0
	}
	if in[3] < in[1] {
		sc += 1<<6 - 1<<0
	}
	if in[4] < in[1] {
		sd += 1<<9 - 1<<0
	}
	if in[5] < in[1] {
		sa += 1<<12 - 1<<0
	}
	if in[6] < in[1] {
		sb += 1<<15 - 1<<0
	}
	if in[7] < in[1] {
		sc += 1<<18 - 1<<0
	}
	if in[2] < in[0] {
		sd += 1 << 3
	}
	if in[3] < in[2] {
		sa += 1<<6 - 1<<3
	}
	if in[4] < in[2] {
		sb += 1<<9 - 1<<3
	}
	if in[5] < in[2] {
		sc += 1<<12 - 1<<3
	}
	if in[6] < in[2] {
		sd += 1<<15 - 1<<3
	}
	if in[7] < in[2] {
		sa += 1<<18 - 1<<3
	}
	if in[3] < in[0] {
		sb += 1 << 6
	}
	if in[4] < in[3] {
		sc += 1<<9 - 1<<6
	}
	if in[5] < in[3] {
		sd += 1<<12 - 1<<6
	}
	if in[6] < in[3] {
		sa += 1<<15 - 1<<6
	}
	if in[7] < in[3] {
		sb += 1<<18 - 1<<6
	}
	if in[4] < in[0] {
		sc += 1 << 9
	}
	if in[5] < in[4] {
		sd += 1<<12 - 1<<9
	}
	if in[6] < in[4] {
		sa += 1<<15 - 1<<9
	}
	if in[7] < in[4] {
		sb += 1<<18 - 1<<9
	}
	if in[5] < in[0] {
		sc += 1 << 12
	}
	if in[6] < in[5] {
		sd += 1<<15 - 1<<12
	}
	if in[7] < in[5] {
		sa += 1<<18 - 1<<12
	}
	if in[6] < in[0] {
		sb += 1 << 15
	}
	if in[7] < in[6] {
		sc += 1<<18 - 1<<15
	}
	if in[7] < in[0] {
		sd += 1 << 18
	}

	s := 0b111110101100011010001 - (sa + sb + sc + sd)

	// TODO: is this a pshufb?
	const bits = 3
	out[s>>(bits*0)%8] = 1
	out[s>>(bits*1)%8] = 2
	out[s>>(bits*2)%8] = 3
	out[s>>(bits*3)%8] = 4
	out[s>>(bits*4)%8] = 5
	out[s>>(bits*5)%8] = 6
	out[s>>(bits*6)%8] = 7

	return out
}
