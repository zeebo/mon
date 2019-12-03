package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
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

const sorted = true
const valueSize = 8
const numKeys = 1 << 20

var largeKey = "57389576498567394"

const keyLength = 8

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
	return keybuf[keyLength*(i%numKeys) : keyLength*((i%numKeys)+1)]
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
		resetStats()

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
			b.ReportMetric(inserting.Seconds()/float64(b.N), "insertion-seconds")
			b.ReportMetric(writing.Seconds()/float64(b.N), "writing-seconds")
			b.ReportMetric(float64(written)/time.Duration(writtenDur).Seconds()/1024/1024/float64(b.N), "written/sec")
			b.ReportMetric(float64(read)/time.Duration(readDur).Seconds()/1024/1024/float64(b.N), "read/sec")
			b.ReportMetric(float64(snapshots)/float64(b.N), "snapshots")
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		resetStats()

		value := make([]byte, valueSize)

		b.SetBytes((valueSize + entrySize))
		b.ResetTimer()
		b.ReportAllocs()

		now := time.Now()
		dir, cleanup := tempDir(b)
		defer cleanup()

		lsm, err := New(dir, Options{
			// NoWAL:     true,
			NoWALSync: true,
		})
		assert.NoError(b, err)
		defer lsm.Close()

		var keys uint64
		var ctr uint64

		b.RunParallel(func(pb *testing.PB) {
			var lkeys uint64
			rng := pcg.New(atomic.AddUint64(&ctr, 1))
			for pb.Next() {
				assert.NoError(b, lsm.SetBytes(getKey(int(rng.Uint32())), value))
				lkeys++
			}
			atomic.AddUint64(&keys, lkeys)
		})

		assert.NoError(b, lsm.CompactAndSync())

		b.ReportMetric(float64(keys)/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(time.Since(now).Microseconds())/float64(keys), "us/key")

		if trackStats {
			b.ReportMetric(inserting.Seconds(), "insertion-seconds")
			b.ReportMetric(writing.Seconds(), "writing-seconds")
			b.ReportMetric(float64(written)/time.Duration(writtenDur).Seconds()/1024/1024, "written/sec")
			b.ReportMetric(float64(read)/time.Duration(readDur).Seconds()/1024/1024, "read/sec")
			b.ReportMetric(float64(snapshots)/time.Since(now).Seconds(), "snapshots/sec")
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
