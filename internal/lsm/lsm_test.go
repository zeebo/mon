package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

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
		defer lsm.Close()

		for i := 0; i < 10000; i++ {
			assert.NoError(t, lsm.SetString(fmt.Sprint(pcg.Uint32()), value))
		}
		assert.NoError(t, lsm.CompactAndSync())
	})
}

const sorted = false
const valueSize = 10
const numKeys = 1 << 20

// const largeKey = "57389576498567394"

const largeKey = ""

const keyLength = 10

var keybuf []byte

func init() {
	var rng pcg.T
	for i := 0; i < numKeys; i++ {
		var key [keyLength]byte
		copy(key[:], []byte(fmt.Sprintf("%d%s", rng.Uint32(), largeKey)))
		_ = key[keyLength-1]
		keybuf = append(keybuf, key[:]...)
	}
	if sorted {
		sort.Sort(inlineKeys(keybuf))
	}
}

func getKey(i int) []byte {
	return keybuf[keyLength*i : keyLength*(i+1)]
}

type inlineKeys []byte

func (ik inlineKeys) Len() int { return numKeys }

func (ik inlineKeys) Less(i int, j int) bool {
	return bytes.Compare(getKey(i), getKey(j)) < 0
}

func (ik inlineKeys) Swap(i int, j int) {
	var tmp [keyLength]byte
	copy(tmp[:], getKey(i))
	copy(getKey(i), getKey(j))
	copy(getKey(j), tmp[:])
}

func BenchmarkLSM(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		inserting, writing = 0, 0
		written, writtenDur = 0, 0
		read, readDur = 0, 0

		value := make([]byte, valueSize)

		b.SetBytes((valueSize + entrySize) * numKeys)
		b.ResetTimer()
		b.ReportAllocs()

		now := time.Now()
		for i := 0; i < b.N; i++ {
			func() {
				dir, cleanup := tempDir(b)
				defer cleanup()

				// defer func() {
				// 	matches, _ := filepath.Glob(dir + "/*")
				// 	for _, path := range matches {
				// 		stat, _ := os.Stat(path)
				// 		b.Logf("% 8d %s", stat.Size(), path)
				// 	}
				// }()

				lsm, err := New(dir, Options{
					NoWALSync: true,
				})
				assert.NoError(b, err)
				defer lsm.Close()

				for i := 0; i < numKeys; i++ {
					assert.NoError(b, lsm.SetBytes(getKey(i), value))
				}
				assert.NoError(b, lsm.CompactAndSync())
			}()
		}

		b.ReportMetric(float64(b.N)*numKeys/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(time.Since(now).Microseconds())/(float64(b.N)*numKeys), "us/key")

		if trackStats {
			b.ReportMetric(inserting.Seconds(), "insertion-seconds")
			b.ReportMetric(writing.Seconds(), "writing-seconds")
			b.ReportMetric(float64(written)/time.Duration(writtenDur).Seconds()/1024/1024, "written/sec")
			b.ReportMetric(float64(read)/time.Duration(readDur).Seconds()/1024/1024, "read/sec")
		}
	})
}

func BenchmarkBolt(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		value := make([]byte, valueSize)

		b.SetBytes((valueSize + entrySize) * numKeys)
		b.ResetTimer()
		b.ReportAllocs()

		now := time.Now()
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

				for i := 0; i < numKeys; i += 1024 {
					assert.NoError(b, db.Update(func(tx *bolt.Tx) error {
						bkt := tx.Bucket(bucket)
						for j := 0; j < 1024; j++ {
							if err := bkt.Put(getKey(i+j), value); err != nil {
								return err
							}
						}
						return nil
					}))
				}
			}()
		}

		b.ReportMetric(float64(b.N)*numKeys/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(time.Since(now).Microseconds())/(float64(b.N)*numKeys), "us/key")
	})
}
