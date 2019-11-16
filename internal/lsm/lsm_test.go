package lsm

import (
	"fmt"
	"os"
	"path/filepath"
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
				fmt.Println(path, stat.Size())
			}
		}()

		lsm, err := New(dir, Options{})
		assert.NoError(t, err)

		value := make([]byte, 124)
		for i := 0; i < 100000; i++ {
			assert.NoError(t, lsm.Set(fmt.Sprint(pcg.Uint32()), value))
		}
	})
}

func BenchmarkLSM(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		dir, cleanup := tempDir(b)
		defer cleanup()

		lsm, err := New(dir, Options{})
		assert.NoError(b, err)

		value := make([]byte, 124)

		b.SetBytes(int64(len(value)))
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			assert.NoError(b, lsm.Set(fmt.Sprint(pcg.Uint32()), value))
		}
	})
}

func BenchmarkBolt(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
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

		value := make([]byte, 124)

		b.SetBytes(int64(len(value)))
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			assert.NoError(b, db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(bucket).Put([]byte(fmt.Sprint(pcg.Uint32())), value)
			}))
		}
	})
}
