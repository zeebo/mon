// +build !gen

package avo

import (
	"testing"

	"github.com/zeebo/pcg"
)

var (
	hole uint64
	buf  [64]uint32
)

//go:noescape
func sum_histogram(*[64]uint32) uint64

func TestAVX(t *testing.T) {
	for i := 0; i < 1000; i++ {
		total := uint64(0)
		for j := range buf {
			buf[j] = pcg.Uint32()
			total += uint64(buf[j])
		}

		if sum_histogram(&buf) != total {
			t.Fatal("wrong answer")
		}
	}
}

func BenchmarkAVX(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hole += sum_histogram(&buf)
	}
}

func BenchmarkInline(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, v := range buf {
			hole += uint64(v)
		}
	}
}

func BenchmarkInlineUnroll8Shard(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var totals [8]uint64
		for j := 0; j <= 56; j += 8 {
			totals[0] += uint64(buf[j+0])
			totals[1] += uint64(buf[j+1])
			totals[2] += uint64(buf[j+2])
			totals[3] += uint64(buf[j+3])
			totals[4] += uint64(buf[j+4])
			totals[5] += uint64(buf[j+5])
			totals[6] += uint64(buf[j+6])
			totals[7] += uint64(buf[j+7])
		}
		hole += totals[0]
		hole += totals[1]
		hole += totals[2]
		hole += totals[3]
		hole += totals[4]
		hole += totals[5]
		hole += totals[6]
		hole += totals[7]
	}
}

func BenchmarkInlineUnroll8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j <= 56; j += 8 {
			hole += uint64(buf[j+0])
			hole += uint64(buf[j+1])
			hole += uint64(buf[j+2])
			hole += uint64(buf[j+3])
			hole += uint64(buf[j+4])
			hole += uint64(buf[j+5])
			hole += uint64(buf[j+6])
			hole += uint64(buf[j+7])
		}
	}
}

func BenchmarkInlineUnroll64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hole += uint64(buf[0+0])
		hole += uint64(buf[0+1])
		hole += uint64(buf[0+2])
		hole += uint64(buf[0+3])
		hole += uint64(buf[0+4])
		hole += uint64(buf[0+5])
		hole += uint64(buf[0+6])
		hole += uint64(buf[0+7])

		hole += uint64(buf[1*8+0])
		hole += uint64(buf[1*8+1])
		hole += uint64(buf[1*8+2])
		hole += uint64(buf[1*8+3])
		hole += uint64(buf[1*8+4])
		hole += uint64(buf[1*8+5])
		hole += uint64(buf[1*8+6])
		hole += uint64(buf[1*8+7])

		hole += uint64(buf[2*8+0])
		hole += uint64(buf[2*8+1])
		hole += uint64(buf[2*8+2])
		hole += uint64(buf[2*8+3])
		hole += uint64(buf[2*8+4])
		hole += uint64(buf[2*8+5])
		hole += uint64(buf[2*8+6])
		hole += uint64(buf[2*8+7])

		hole += uint64(buf[3*8+0])
		hole += uint64(buf[3*8+1])
		hole += uint64(buf[3*8+2])
		hole += uint64(buf[3*8+3])
		hole += uint64(buf[3*8+4])
		hole += uint64(buf[3*8+5])
		hole += uint64(buf[3*8+6])
		hole += uint64(buf[3*8+7])

		hole += uint64(buf[4*8+0])
		hole += uint64(buf[4*8+1])
		hole += uint64(buf[4*8+2])
		hole += uint64(buf[4*8+3])
		hole += uint64(buf[4*8+4])
		hole += uint64(buf[4*8+5])
		hole += uint64(buf[4*8+6])
		hole += uint64(buf[4*8+7])

		hole += uint64(buf[5*8+0])
		hole += uint64(buf[5*8+1])
		hole += uint64(buf[5*8+2])
		hole += uint64(buf[5*8+3])
		hole += uint64(buf[5*8+4])
		hole += uint64(buf[5*8+5])
		hole += uint64(buf[5*8+6])
		hole += uint64(buf[5*8+7])

		hole += uint64(buf[6*8+0])
		hole += uint64(buf[6*8+1])
		hole += uint64(buf[6*8+2])
		hole += uint64(buf[6*8+3])
		hole += uint64(buf[6*8+4])
		hole += uint64(buf[6*8+5])
		hole += uint64(buf[6*8+6])
		hole += uint64(buf[6*8+7])

		hole += uint64(buf[7*8+0])
		hole += uint64(buf[7*8+1])
		hole += uint64(buf[7*8+2])
		hole += uint64(buf[7*8+3])
		hole += uint64(buf[7*8+4])
		hole += uint64(buf[7*8+5])
		hole += uint64(buf[7*8+6])
		hole += uint64(buf[7*8+7])
	}
}
