package testutil

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/zeebo/pcg"
)

const (
	NumKeys = 1 << 20

	sorted    = false
	largeKey  = "57389576498567394"
	keyLength = 8
)

var keybuf []byte

var keyOnce sync.Once

func initKeys() {
	var rng pcg.T
	for i := 0; i < NumKeys; i++ {
		var key [keyLength]byte
		copy(key[:], []byte(fmt.Sprintf("%d%s", rng.Uint32(), largeKey)))
		_ = key[keyLength-1]
		keybuf = append(keybuf, key[:]...)
	}
	if sorted {
		sort.Sort(inlineKeys(keybuf))
	}
}

func GetKey(i int) []byte {
	keyOnce.Do(initKeys)
	return keybuf[keyLength*(i%NumKeys) : keyLength*((i%NumKeys)+1)]
}

type inlineKeys []byte

func (ik inlineKeys) Len() int { return NumKeys }

func (ik inlineKeys) Less(i int, j int) bool {
	return bytes.Compare(GetKey(i), GetKey(j)) < 0
}

func (ik inlineKeys) Swap(i int, j int) {
	var tmp [keyLength]byte
	copy(tmp[:], GetKey(i))
	copy(GetKey(i), GetKey(j))
	copy(GetKey(j), tmp[:])
}
