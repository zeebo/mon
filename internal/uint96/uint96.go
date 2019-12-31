package uint96

import "github.com/zeebo/wyhash"

type T struct {
	H uint64
	L uint32
}

func Less(s, t T) bool {
	return s.H < t.H || (s.H == t.H && s.L < t.L)
}

func Hash(data []byte) T {
	return T{
		H: wyhash.Hash(data, 0),
		L: uint32(wyhash.Hash(data, 1)),
	}
}

func HashString(data string) T {
	return T{
		H: wyhash.HashString(data, 0),
		L: uint32(wyhash.HashString(data, 1)),
	}
}
