// +build debug

package inthist

import (
	"fmt"
	"strings"
	"sync/atomic"
)

func (h *Histogram) Bitmap() string {
	var lines []string
	for bucket := range h.buckets[:] {
		b := loadBucket(&h.buckets[bucket])
		if b == nil {
			lines = append(lines, strings.Repeat("0", histEntries))
			continue
		}

		var line []byte
		for entry := range b.entries[:] {
			count := atomic.LoadUint32(&b.entries[entry])
			if count == 0 {
				line = append(line, '0')
			} else {
				line = append(line, '1')
			}
		}
		lines = append(lines, string(line))
	}
	return strings.Join(lines, "\n") + "\n"
}

func (h *Histogram) Dump() {
	for bucket := range h.buckets[:] {
		b := loadBucket(&h.buckets[bucket])
		if b == nil {
			continue
		}

		for entry := range b.entries[:] {
			count := atomic.LoadUint32(&b.entries[entry])
			if count == 0 {
				continue
			}

			fmt.Printf("%d:%d\n", lowerValue(uint64(bucket), uint64(entry)), count)
		}
	}
}
