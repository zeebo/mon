package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/zeebo/assert"
	"github.com/zeebo/mon/internal/lsm/entry"
	"github.com/zeebo/mon/internal/lsm/mem/skipmem"
	"github.com/zeebo/mon/internal/lsm/testutil"
	"github.com/zeebo/pcg"
)

func TestLSM(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		dir, cleanup := testutil.TempDir(t)
		defer cleanup()

		defer func() {
			matches, _ := filepath.Glob(dir + "/*")
			for _, path := range matches {
				stat, _ := os.Stat(path)
				t.Logf("% 8d %s", stat.Size(), path)
			}
		}()

		value := make([]byte, testutil.ValueLength)
		lsm, err := New(dir, Options{
			MemCap:    4096,
			NoWALSync: true,
		})
		assert.NoError(t, err)
		defer lsm.Close()

		for i := 0; i < 10000; i++ {
			assert.NoError(t, lsm.SetBytes(testutil.GetKey(i), value))
		}
		assert.NoError(t, lsm.CompactAndSync())
	})
}

func BenchmarkLSM(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		value := make([]byte, testutil.ValueLength)

		b.SetBytes((testutil.ValueLength + entry.Size) * testutil.NumKeys)
		b.ResetTimer()
		b.ReportAllocs()

		now := time.Now()
		for i := 0; i < b.N; i++ {
			func() {
				dir, cleanup := testutil.TempDir(b)
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

				for i := 0; i < testutil.NumKeys; i++ {
					assert.NoError(b, lsm.SetBytes(testutil.GetKey(i), value))
				}
				assert.NoError(b, lsm.CompactAndSync())
			}()
		}

		b.ReportMetric(float64(b.N)*testutil.NumKeys/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(time.Since(now).Microseconds())/(float64(b.N)*testutil.NumKeys), "us/key")

		fmt.Println(skipmem.Buckets)
	})

	b.Run("Parallel", func(b *testing.B) {
		value := make([]byte, testutil.ValueLength)

		b.SetBytes((testutil.ValueLength + entry.Size))
		b.ResetTimer()
		b.ReportAllocs()

		now := time.Now()
		dir, cleanup := testutil.TempDir(b)
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
				assert.NoError(b, lsm.SetBytes(testutil.GetKey(int(rng.Uint32())), value))
				lkeys++
			}
			atomic.AddUint64(&keys, lkeys)
		})

		assert.NoError(b, lsm.CompactAndSync())

		b.ReportMetric(float64(keys)/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(time.Since(now).Microseconds())/float64(keys), "us/key")
	})
}

func BenchmarkBolt(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		value := make([]byte, testutil.ValueLength)

		b.SetBytes((testutil.ValueLength + entry.Size) * testutil.NumKeys)
		b.ResetTimer()
		b.ReportAllocs()

		now := time.Now()
		for i := 0; i < b.N; i++ {
			func() {
				dir, cleanup := testutil.TempDir(b)
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

				for i := 0; i < testutil.NumKeys; i += 1024 {
					assert.NoError(b, db.Update(func(tx *bolt.Tx) error {
						bkt := tx.Bucket(bucket)
						for j := 0; j < 1024; j++ {
							if err := bkt.Put(testutil.GetKey(i+j), value); err != nil {
								return err
							}
						}
						return nil
					}))
				}
			}()
		}

		b.ReportMetric(float64(b.N)*testutil.NumKeys/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(time.Since(now).Microseconds())/(float64(b.N)*testutil.NumKeys), "us/key")
	})
}
