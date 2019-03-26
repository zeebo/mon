package monflux

import (
	"fmt"
	"io"
	"math"
	"sync/atomic"

	"github.com/zeebo/mon"
)

type Collector struct {
	Measurement       string
	ExcludeHistograms bool
}

func (c Collector) Write(w io.Writer) error {
	ew := &errWriter{w: w}
	mon.Times(func(name string, state *mon.State) bool {
		var m string
		if c.Measurement != "" {
			m = fmt.Sprintf("%q,name=%q", c.Measurement, name)
		} else {
			m = fmt.Sprintf("%q", name)
		}

		current, total := state.Current(), state.Total()
		fmt.Fprintf(ew, "%s current=%di\n", m, current)
		fmt.Fprintf(ew, "%s total=%di\n", m, total)
		for iter := state.Errors().Iterator(); iter.Next(); {
			err, count := iter.Key(), atomic.LoadInt64((*int64)(iter.Value()))
			fmt.Fprintf(ew, "%s,error=%q count=%di\n", m, err, count)
		}

		if _, average := state.Average(); !math.IsNaN(average) {
			fmt.Fprintf(ew, "%s average=%v\n", m, average/1e9)

			if !c.ExcludeHistograms {
				his := state.Histogram()
				outputq := func(q float64) {
					value := float64(his.Quantile(q)) / 1e9
					fmt.Fprintf(ew, "%s,percentile=%v value=%v\n", m, q, value)
				}

				outputq(0)
				outputq(0.5)
				for i, p := int64(10), float64(0.1); i/2 < total; i, p = i*10, p/10 {
					outputq(1 - p)
				}
				outputq(1)
			}
		}

		return ew.err == nil
	})
	return ew.err
}

func Write(w io.Writer) error { return Collector{}.Write(w) }

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
