package lsm

import "time"

const trackStats = true

var (
	inserting time.Duration
	writing   time.Duration

	written    int64
	writtenDur int64

	read    int64
	readDur int64

	snapshots int64
)

func resetStats() {
	inserting, writing = 0, 0
	written, writtenDur = 0, 0
	read, readDur = 0, 0
	snapshots = 0
}
