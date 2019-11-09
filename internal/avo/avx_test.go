// +build !gen

package avo

import (
	"testing"
	"unsafe"

	"github.com/zeebo/pcg"
)

const size = 64

var (
	hole uint64
	buf  [size]uint64
)

//go:noescape
func sum_histogram64(*[size]uint64) uint64

func TestAVX(t *testing.T) {
	for i := 0; i < 1000; i++ {
		total := uint64(0)
		for j := range buf {
			buf[j] = pcg.Uint64()
			total += uint64(buf[j])
		}

		if sum_histogram64(&buf) != total ||
			sum64(&buf) != total ||
			sum64_unroll(&buf) != total ||
			sum64_unroll_pointer(&buf) != total ||
			sum64_unroll_full(&buf) != total {

			t.Fatal("wrong answer")
		}
	}
}

func BenchmarkAVX(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hole += sum_histogram64(&buf)
	}
}

func BenchmarkGo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hole += sum64(&buf)
	}
}

func sum64(buf *[size]uint64) (tmp uint64) {
	for _, v := range buf {
		tmp += uint64(v)
	}
	return tmp
}

func BenchmarkGoUnroll(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hole += sum64_unroll(&buf)
	}
}

func sum64_unroll(buf *[size]uint64) (tmp uint64) {
	for j := 0; j <= size-8; j += 8 {
		tmp += uint64(buf[j+0])
		tmp += uint64(buf[j+1])
		tmp += uint64(buf[j+2])
		tmp += uint64(buf[j+3])
		tmp += uint64(buf[j+4])
		tmp += uint64(buf[j+5])
		tmp += uint64(buf[j+6])
		tmp += uint64(buf[j+7])
	}
	return tmp
}

func BenchmarkGoUnrollPointer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hole += sum64_unroll_pointer(&buf)
	}
}

func sum64_unroll_pointer(buf *[size]uint64) (tmp uint64) {
	base := unsafe.Pointer(&buf[0])
	j := 0

next:
	tmp += *(*uint64)(unsafe.Pointer(uintptr(base) + 0*8))
	tmp += *(*uint64)(unsafe.Pointer(uintptr(base) + 1*8))
	tmp += *(*uint64)(unsafe.Pointer(uintptr(base) + 2*8))
	tmp += *(*uint64)(unsafe.Pointer(uintptr(base) + 3*8))
	tmp += *(*uint64)(unsafe.Pointer(uintptr(base) + 4*8))
	tmp += *(*uint64)(unsafe.Pointer(uintptr(base) + 5*8))
	tmp += *(*uint64)(unsafe.Pointer(uintptr(base) + 6*8))
	tmp += *(*uint64)(unsafe.Pointer(uintptr(base) + 7*8))

	if j < 8 {
		j++
		base = unsafe.Pointer(uintptr(base) + 64)
		goto next
	}

	return tmp
}

func BenchmarkGoUnrollFull(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hole += sum64_unroll_full(&buf)
	}
}

func sum64_unroll_full(buf *[size]uint64) (tmp uint64) {
	tmp += uint64(buf[0+0])
	tmp += uint64(buf[0+1])
	tmp += uint64(buf[0+2])
	tmp += uint64(buf[0+3])
	tmp += uint64(buf[0+4])
	tmp += uint64(buf[0+5])
	tmp += uint64(buf[0+6])
	tmp += uint64(buf[0+7])

	tmp += uint64(buf[1*8+0])
	tmp += uint64(buf[1*8+1])
	tmp += uint64(buf[1*8+2])
	tmp += uint64(buf[1*8+3])
	tmp += uint64(buf[1*8+4])
	tmp += uint64(buf[1*8+5])
	tmp += uint64(buf[1*8+6])
	tmp += uint64(buf[1*8+7])

	tmp += uint64(buf[2*8+0])
	tmp += uint64(buf[2*8+1])
	tmp += uint64(buf[2*8+2])
	tmp += uint64(buf[2*8+3])
	tmp += uint64(buf[2*8+4])
	tmp += uint64(buf[2*8+5])
	tmp += uint64(buf[2*8+6])
	tmp += uint64(buf[2*8+7])

	tmp += uint64(buf[3*8+0])
	tmp += uint64(buf[3*8+1])
	tmp += uint64(buf[3*8+2])
	tmp += uint64(buf[3*8+3])
	tmp += uint64(buf[3*8+4])
	tmp += uint64(buf[3*8+5])
	tmp += uint64(buf[3*8+6])
	tmp += uint64(buf[3*8+7])

	tmp += uint64(buf[4*8+0])
	tmp += uint64(buf[4*8+1])
	tmp += uint64(buf[4*8+2])
	tmp += uint64(buf[4*8+3])
	tmp += uint64(buf[4*8+4])
	tmp += uint64(buf[4*8+5])
	tmp += uint64(buf[4*8+6])
	tmp += uint64(buf[4*8+7])

	tmp += uint64(buf[5*8+0])
	tmp += uint64(buf[5*8+1])
	tmp += uint64(buf[5*8+2])
	tmp += uint64(buf[5*8+3])
	tmp += uint64(buf[5*8+4])
	tmp += uint64(buf[5*8+5])
	tmp += uint64(buf[5*8+6])
	tmp += uint64(buf[5*8+7])

	tmp += uint64(buf[6*8+0])
	tmp += uint64(buf[6*8+1])
	tmp += uint64(buf[6*8+2])
	tmp += uint64(buf[6*8+3])
	tmp += uint64(buf[6*8+4])
	tmp += uint64(buf[6*8+5])
	tmp += uint64(buf[6*8+6])
	tmp += uint64(buf[6*8+7])

	tmp += uint64(buf[7*8+0])
	tmp += uint64(buf[7*8+1])
	tmp += uint64(buf[7*8+2])
	tmp += uint64(buf[7*8+3])
	tmp += uint64(buf[7*8+4])
	tmp += uint64(buf[7*8+5])
	tmp += uint64(buf[7*8+6])
	tmp += uint64(buf[7*8+7])

	return tmp
}
