package monprom

import (
	"math"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/zeebo/mon"
)

var (
	nameLabel  = "name"
	errorLabel = "error"
)

func newDesc(name, help string, labels ...string) *prometheus.Desc {
	ls := append([]string{nameLabel}, labels...)
	return prometheus.NewDesc("mon_"+name, help, ls, nil)
}

var (
	descCurrent   = newDesc("current", "Currently active")
	descTotal     = newDesc("total", "Total executed")
	descErrors    = newDesc("errors", "Count of errors", errorLabel)
	descAverage   = newDesc("average", "Average of monitored time")
	descHistogram = newDesc("histogram", "Histogram of monitored times (milliseconds)")
)

type Collector struct {
	ExcludeHistograms bool
}

func (c Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descCurrent
	ch <- descTotal
	ch <- descErrors
	ch <- descAverage
	if !c.ExcludeHistograms {
		ch <- descHistogram
	}
}

func (c Collector) Collect(metrics chan<- prometheus.Metric) {
	mon.Times(func(name string, state *mon.State) bool {
		lp := []*dto.LabelPair{{Name: &nameLabel, Value: &name}}
		_, average := state.Average()
		metrics <- &metric{desc: descCurrent, lp: lp, float64: float64(state.Current())}
		metrics <- &metric{desc: descTotal, lp: lp, float64: float64(state.Total())}
		for iter := state.Errors().Iterator(); iter.Next(); {
			name := iter.Key()
			errcount := atomic.LoadInt64((*int64)(iter.Value()))
			lp = append(lp[:1], &dto.LabelPair{Name: &errorLabel, Value: &name})
			metrics <- &metric{desc: descErrors, lp: lp, float64: float64(errcount)}
		}
		if !math.IsNaN(average) {
			metrics <- &metric{desc: descAverage, lp: lp, float64: average / 1e9}
			if !c.ExcludeHistograms {
				metrics <- &metric{desc: descHistogram, lp: lp, histogram: state.Histogram()}
			}
		}
		return true
	})
}

type metric struct {
	desc      *prometheus.Desc
	lp        []*dto.LabelPair
	float64   float64
	histogram *mon.Histogram
}

func (m *metric) Desc() *prometheus.Desc { return m.desc }

func (m *metric) Write(o *dto.Metric) error {
	o.Label = m.lp

	switch m.desc {
	case descCurrent, descAverage:
		o.Gauge = &dto.Gauge{Value: &m.float64}

	case descTotal, descErrors:
		o.Counter = &dto.Counter{Value: &m.float64}

	case descHistogram:
		his := &dto.Histogram{
			SampleCount: new(uint64),
			SampleSum:   new(float64),
		}

		prevCount := 0.0
		m.histogram.Percentiles(func(value, count, total int64) {
			// Update SampleSum and SampleCount
			fcount, fvalue := float64(count), float64(value)
			*his.SampleSum += (fcount - prevCount) * fvalue
			*his.SampleCount = uint64(total)
			prevCount = fcount

			// Add a bucket
			ucount := uint64(count)
			his.Bucket = append(his.Bucket, &dto.Bucket{
				CumulativeCount: &ucount,
				UpperBound:      &fvalue,
			})
		})

		o.Histogram = his
	}

	return nil
}
