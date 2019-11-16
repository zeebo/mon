package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestLSM(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		dir, cleanup := tempDir(t)
		defer cleanup()

		defer func() {
			matches, _ := filepath.Glob(dir + "/*")
			for _, path := range matches {
				stat, _ := os.Stat(path)
				t.Logf("% 8d %s", stat.Size(), path)
			}
		}()

		value := make([]byte, valueSize)
		lsm, err := New(dir, Options{
			MemCap:    4096,
			NoWALSync: true,
		})
		assert.NoError(t, err)

		for i := 0; i < 10000; i++ {
			assert.NoError(t, lsm.Set(fmt.Sprint(pcg.Uint32()), value))
		}
	})
}

const valueSize = 128
const numKeys = 1 << 20

var keys [numKeys]string
var keysb [numKeys][]byte

func init() {
	for i := range keys {
		keys[i] = fmt.Sprint(pcg.Uint32())
		keysb[i] = []byte(keys[i])
	}
}

func BenchmarkLSM(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		dir, cleanup := tempDir(b)
		defer cleanup()

		lsm, err := New(dir, Options{
			NoWALSync: true,
		})
		assert.NoError(b, err)

		value := make([]byte, valueSize)

		b.SetBytes(valueSize)
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			assert.NoError(b, lsm.Set(keys[pcg.Uint32n(numKeys)%numKeys], value))
		}
	})
}

// func BenchmarkBolt(b *testing.B) {
// 	b.Run("Basic", func(b *testing.B) {
// 		dir, cleanup := tempDir(b)
// 		defer cleanup()

// 		db, err := bolt.Open(filepath.Join(dir, "foo"), 0644, nil)
// 		assert.NoError(b, err)
// 		defer db.Close()
// 		bucket := []byte("bucket")

// 		db.NoSync = true

// 		assert.NoError(b, db.Update(func(tx *bolt.Tx) error {
// 			_, err := tx.CreateBucket(bucket)
// 			return err
// 		}))

// 		value := make([]byte, valueSize)

// 		b.SetBytes(valueSize)
// 		b.ResetTimer()
// 		b.ReportAllocs()

// 		for i := 0; i < b.N; i++ {
// 			assert.NoError(b, db.Update(func(tx *bolt.Tx) error {
// 				return tx.Bucket(bucket).Put(keysb[pcg.Uint32n(numKeys)%numKeys], value)
// 			}))
// 		}
// 	})
// }
