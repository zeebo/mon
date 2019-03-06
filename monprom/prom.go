package monprom

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/zeebo/mon"
)

var nameLabel = "name"

func newDesc(name, help string) *prometheus.Desc {
	return prometheus.NewDesc("mon_"+name, help, []string{nameLabel}, nil)
}

var (
	descCurrent   = newDesc("current", "Currently active")
	descTotal     = newDesc("total", "Total executed")
	descAverage   = newDesc("average", "Average of monitored time")
	descHistogram = newDesc("histogram", "Histogram of monitored times (milliseconds)")
)

type Collector struct {
	ExcludeHistograms bool
}

func (c Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descCurrent
	ch <- descTotal
	ch <- descAverage
	if !c.ExcludeHistograms {
		ch <- descHistogram
	}
}

func (c Collector) Collect(metrics chan<- prometheus.Metric) {
	mon.Times(func(name string, state *mon.State) bool {
		lp := []*dto.LabelPair{{Name: &nameLabel, Value: &name}}
		_, average := state.Average()
		metrics <- &metric{desc: descCurrent, lp: lp, current: float64(state.Current())}
		metrics <- &metric{desc: descTotal, lp: lp, total: float64(state.Total())}
		if !math.IsNaN(average) {
			metrics <- &metric{desc: descAverage, lp: lp, average: average / 1e9}
			if !c.ExcludeHistograms {
				metrics <- &metric{desc: descHistogram, lp: lp, histogram: state.Histogram()}
			}
		}
		return true
	})
}

type metric struct {
	desc *prometheus.Desc
	lp   []*dto.LabelPair

	// depending on desc
	current   float64
	total     float64
	average   float64
	variance  float64
	histogram *mon.Histogram
}

func (m *metric) Desc() *prometheus.Desc { return m.desc }

func (m *metric) Write(o *dto.Metric) error {
	o.Label = m.lp

	switch m.desc {
	case descCurrent:
		o.Gauge = &dto.Gauge{Value: &m.current}

	case descTotal:
		o.Counter = &dto.Counter{Value: &m.total}

	case descAverage:
		o.Gauge = &dto.Gauge{Value: &m.average}

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
