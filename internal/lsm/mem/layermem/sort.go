package layermem

func sortEntries4(in *[4]layerEntry) (out [4]uint8) {
	var s uint16 = 0b111001

	if in[1].prefix < in[0].prefix {
		s -= 1 << 0
	}
	if in[2].prefix < in[1].prefix {
		s -= 1<<2 - 1<<0
	}
	if in[3].prefix < in[1].prefix {
		s -= 1<<4 - 1<<0
	}
	if in[2].prefix < in[0].prefix {
		s -= 1 << 2
	}
	if in[3].prefix < in[2].prefix {
		s -= 1<<4 - 1<<2
	}
	if in[3].prefix < in[0].prefix {
		s -= 1 << 4
	}

	const bits = 2
	out[s>>(bits*0)%4] = 1
	out[s>>(bits*1)%4] = 2
	out[s>>(bits*2)%4] = 3

	return out
}

func min2Entries4(in *[4]layerEntry) (a, b uint8) {
	av, bv := in[0].prefix, in[1].prefix
	ai, bi := uint64(0), uint64(1)

	if bv < av {
		bi, bv, ai, av = ai, av, bi, bv
	}

	if in[2].prefix < av {
		bi, bv, ai, av = ai, av, 2, in[2].prefix
	} else if in[2].prefix < bv {
		bi, bv = 2, in[2].prefix
	}

	if in[3].prefix < av {
		bi, ai = ai, 3
	} else if in[3].prefix < bv {
		bi = 3
	}

	return byte(ai), byte(bi)
}

func sortEntries8(in *[8]layerEntry) (out [8]uint8) {
	var sa, sb, sc, sd uint32

	if in[1].prefix < in[0].prefix {
		sa += 1 << 0
	}
	if in[2].prefix < in[1].prefix {
		sb += 1<<3 - 1<<0
	}
	if in[3].prefix < in[1].prefix {
		sc += 1<<6 - 1<<0
	}
	if in[4].prefix < in[1].prefix {
		sd += 1<<9 - 1<<0
	}
	if in[5].prefix < in[1].prefix {
		sa += 1<<12 - 1<<0
	}
	if in[6].prefix < in[1].prefix {
		sb += 1<<15 - 1<<0
	}
	if in[7].prefix < in[1].prefix {
		sc += 1<<18 - 1<<0
	}
	if in[2].prefix < in[0].prefix {
		sd += 1 << 3
	}
	if in[3].prefix < in[2].prefix {
		sa += 1<<6 - 1<<3
	}
	if in[4].prefix < in[2].prefix {
		sb += 1<<9 - 1<<3
	}
	if in[5].prefix < in[2].prefix {
		sc += 1<<12 - 1<<3
	}
	if in[6].prefix < in[2].prefix {
		sd += 1<<15 - 1<<3
	}
	if in[7].prefix < in[2].prefix {
		sa += 1<<18 - 1<<3
	}
	if in[3].prefix < in[0].prefix {
		sb += 1 << 6
	}
	if in[4].prefix < in[3].prefix {
		sc += 1<<9 - 1<<6
	}
	if in[5].prefix < in[3].prefix {
		sd += 1<<12 - 1<<6
	}
	if in[6].prefix < in[3].prefix {
		sa += 1<<15 - 1<<6
	}
	if in[7].prefix < in[3].prefix {
		sb += 1<<18 - 1<<6
	}
	if in[4].prefix < in[0].prefix {
		sc += 1 << 9
	}
	if in[5].prefix < in[4].prefix {
		sd += 1<<12 - 1<<9
	}
	if in[6].prefix < in[4].prefix {
		sa += 1<<15 - 1<<9
	}
	if in[7].prefix < in[4].prefix {
		sb += 1<<18 - 1<<9
	}
	if in[5].prefix < in[0].prefix {
		sc += 1 << 12
	}
	if in[6].prefix < in[5].prefix {
		sd += 1<<15 - 1<<12
	}
	if in[7].prefix < in[5].prefix {
		sa += 1<<18 - 1<<12
	}
	if in[6].prefix < in[0].prefix {
		sb += 1 << 15
	}
	if in[7].prefix < in[6].prefix {
		sc += 1<<18 - 1<<15
	}
	if in[7].prefix < in[0].prefix {
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
