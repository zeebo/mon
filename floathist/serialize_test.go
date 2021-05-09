package floathist

import (
	"encoding/hex"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestSerialize(t *testing.T) {
	t.Run("Write", func(t *testing.T) {
		h := new(Histogram)
		for i := int64(0); i < 10000; i++ {
			r := float32(pcg.Uint32n(1000) + 500)
			h.Observe(r)
		}

		data := h.Serialize(nil)
		t.Logf("%d\n%s", len(data), hex.Dump(data))
	})

	t.Run("Load", func(t *testing.T) {
		h := new(Histogram)
		for i := int64(0); i < 10000; i++ {
			r := float32(pcg.Uint32n(1000) + 500)
			h.Observe(r)
		}

		h2 := new(Histogram)
		assert.NoError(t, h2.Load(h.Serialize(nil)))

		assert.Equal(t, h.Total(), h2.Total())
		assert.Equal(t, h.Sum(), h2.Sum())
		t.Log(h.Average())
		t.Log(h2.Average())
	})
}

func BenchmarkSerialize(b *testing.B) {
	b.Run("Write", func(b *testing.B) {
		h := new(Histogram)
		for i := int64(0); i < 100000; i++ {
			r := pcg.Float32()
			h.Observe(r)
		}
		buf := h.Serialize(nil)

		b.SetBytes(int64(len(buf)))
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			h.Serialize(buf[:0])
		}

		b.ReportMetric(float64(len(buf)), "bytes")
	})

	b.Run("Load", func(b *testing.B) {
		h := new(Histogram)
		for i := int64(0); i < 100000; i++ {
			r := pcg.Float32()
			h.Observe(r)
		}
		buf := h.Serialize(nil)

		b.SetBytes(int64(len(buf)))
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var h Histogram
			_ = h.Load(buf)
		}

		b.ReportMetric(float64(len(buf)), "bytes")
	})
}
