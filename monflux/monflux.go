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
	outputf := func(m, f string, val float64) { fmt.Fprintf(ew, "%s %s=%v\n", m, f, val) }
	outputi := func(m, f string, val int64) { fmt.Fprintf(ew, "%s %s=%di\n", m, f, val) }
	outputq := func(m string, his *mon.Histogram, q float64) {
		fmt.Fprintf(ew, "%s,percentile=%v value=%v\n", m, q, float64(his.Quantile(q))/1e9)
	}
	mon.Times(func(name string, state *mon.State) bool {
		current, total := state.Current(), state.Total()
		outputi(name, "current", current)
		outputi(name, "total", total)
		for iter := state.Errors().Iterator(); iter.Next(); {
			err, count := iter.Key(), atomic.LoadInt64((*int64)(iter.Value()))
			outputi(fmt.Sprintf("%s,error=%q", name, err), "count", count)
		}
		if _, average := state.Average(); !math.IsNaN(average) {
			outputf(name, "average", average/1e9)
			his := state.Histogram()
			outputq(name, his, 0)
			outputq(name, his, 0.5)
			for i, p := int64(10), float64(0.1); i/2 < total; i, p = i*10, p/10 {
				outputq(name, his, 1-p)
			}
			outputq(name, his, 1)
		}

		return ew.err == nil
	})
	return ew.err
}
