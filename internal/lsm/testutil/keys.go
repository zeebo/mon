package testutil

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/zeebo/pcg"
)

const (
	KeyLength   = 8
	ValueLength = 8
	NumKeys     = 1 << 20

	sorted   = true
	largeKey = "57389576498567394"
)

var keybuf []byte
var keyOnce sync.Once

func initKeys() {
	var rng pcg.T
	for i := 0; i < NumKeys; i++ {
		var key [KeyLength]byte
		copy(key[:], []byte(fmt.Sprintf("%d%s", rng.Uint32(), largeKey)))
		keybuf = append(keybuf, key[:]...)
	}
	if sorted {
		sort.Sort(inlineKeys(keybuf))
	}
}

func GetKey(i int) []byte {
	keyOnce.Do(initKeys)
	return getKey(i)
}

func getKey(i int) []byte {
	return keybuf[KeyLength*(i%NumKeys) : KeyLength*((i%NumKeys)+1)]
}

type inlineKeys []byte

func (ik inlineKeys) Len() int { return NumKeys }

func (ik inlineKeys) Less(i int, j int) bool {
	return bytes.Compare(getKey(i), getKey(j)) < 0
}

func (ik inlineKeys) Swap(i int, j int) {
	var tmp [KeyLength]byte
	copy(tmp[:], getKey(i))
	copy(getKey(i), getKey(j))
	copy(getKey(j), tmp[:])
}
