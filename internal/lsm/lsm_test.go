package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/boltdb/bolt"
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
			assert.NoError(t, lsm.SetString(fmt.Sprint(pcg.Uint32()), value))
		}
	})
}

const sorted = true
const valueSize = 16
const numKeys = 1 << 20

var keys [numKeys]string
var keysb [numKeys][]byte

func init() {
	var rng pcg.T
	for i := range keys {
		keys[i] = fmt.Sprint(rng.Uint32())
		keysb[i] = []byte(keys[i])
	}
	if sorted {
		sort.Strings(keys[:])
		sort.Slice(keysb[:], func(i, j int) bool {
			return bytes.Compare(keysb[i], keysb[j]) == -1
		})
	}
}

func BenchmarkLSM(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		inserting, writing = 0, 0

		value := make([]byte, valueSize)

		b.SetBytes((valueSize + entrySize) * numKeys)
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			func() {
				dir, cleanup := tempDir(b)
				defer cleanup()

				lsm, err := New(dir, Options{
					NoWALSync: true,
				})
				assert.NoError(b, err)

				for _, v := range &keysb {
					assert.NoError(b, lsm.SetBytes(v, value))
				}
			}()
		}

		b.ReportMetric(inserting.Seconds(), "insertion-seconds")
		b.ReportMetric(writing.Seconds(), "writing-seconds")
		b.ReportMetric(numKeys, "keys")
	})
}

func BenchmarkBolt(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		value := make([]byte, valueSize)

		b.SetBytes((valueSize + entrySize) * numKeys)
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			func() {
				dir, cleanup := tempDir(b)
				defer cleanup()

				db, err := bolt.Open(filepath.Join(dir, "foo"), 0644, nil)
				assert.NoError(b, err)
				defer db.Close()
				bucket := []byte("bucket")

				db.NoSync = true

				assert.NoError(b, db.Update(func(tx *bolt.Tx) error {
					_, err := tx.CreateBucket(bucket)
					return err
				}))

				for i := 0; i < len(keysb); i += 1024 {
					assert.NoError(b, db.Update(func(tx *bolt.Tx) error {
						bkt := tx.Bucket(bucket)
						for _, v := range keysb[i : i+1024] {
							if err := bkt.Put(v, value); err != nil {
								return err
							}
						}
						return nil
					}))
				}
			}()
		}

		b.ReportMetric(numKeys, "keys")
	})
}
