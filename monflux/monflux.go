package monflux

import (
	"fmt"
	"io"
	"math"
	"sync/atomic"

	"github.com/zeebo/mon"
)

type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) Write(p []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	var n int
	n, e.err = e.w.Write(p)
	return n, e.err
}

func Write(w io.Writer) error {
	ew := &errWriter{w: w}
	mon.Times(func(name string, state *mon.State) bool {
		current, total := state.Current(), state.Total()
		fmt.Fprintf(ew, "mon,name=%q current=%di\n", name, current)
		fmt.Fprintf(ew, "mon,name=%q total=%di\n", name, total)
		for iter := state.Errors().Iterator(); iter.Next(); {
			err, count := iter.Key(), atomic.LoadInt64((*int64)(iter.Value()))
			fmt.Fprintf(ew, "mon,name=%q,error=%q count=%di\n", name, err, count)
		}

		if _, average := state.Average(); !math.IsNaN(average) {
			fmt.Fprintf(ew, "mon,name=%q average=%v\n", name, average/1e9)

			his := state.Histogram()
			outputq := func(q float64) {
				value := float64(his.Quantile(q)) / 1e9
				fmt.Fprintf(ew, "mon,name=%q,percentile=%v value=%v\n", name, q, value)
			}

			outputq(0)
			outputq(0.5)
			for i, p := int64(10), float64(0.1); i/2 < total; i, p = i*10, p/10 {
				outputq(1 - p)
			}
			outputq(1)
		}

		return ew.err == nil
	})
	return ew.err
}
