package monhandler

import (
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	chart "github.com/wcharczuk/go-chart"
	"github.com/zeebo/mon"
)

// Handler serves information about collected metrics.
type Handler struct{}

func (Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/" {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintln(w, "<table border=1>")
		fmt.Fprintln(w, "<tr><td>name</td><td>current</td><td>total</td><td>sum</td><td>average</td><td>variance</td></tr>")
		mon.Times(func(name string, st *mon.State) bool {
			current, total := st.Current(), st.Total()
			sum, avg, vari := st.Variance()
			fmt.Fprintf(w, `<tr><td><a href="%[1]s">%[1]s</a></td><td>%d</td><td>%d</td><td>%v</td><td>%v</td><td>%v</td></tr>`,
				name, current, total, sum, avg, vari)
			return true
		})
		return
	}

	state := mon.GetState(req.URL.Path[1:])
	if state == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "image/svg+xml")
	makeChart(state.Histogram()).Render(chart.SVG, w)
}

func getLabel(i int) string {
	switch i {
	case 0:
		return "0%"
	case 1:
		return "90%"
	case 2:
		return "99%"
	default:
		return fmt.Sprintf("99.%s%%", strings.Repeat("9", i-2))
	}
}

func makeChart(his *mon.Histogram) *chart.Chart {
	var x, y []float64
	var t float64

	his.Percentiles(func(value, count, total int64) {
		t = float64(total)
		x = append(x, float64(count)/t)
		y = append(y, float64(value))
	})

	// make a log axis
	var p int
	for ti := t; ti >= 1; ti /= 10 {
		p++
	}

	var xticks []chart.Tick
	var xgrids []chart.GridLine
	for i := 0; i < p; i++ {
		xticks = append(xticks, chart.Tick{
			Value: float64(i),
			Label: getLabel(i),
		})
		xgrids = append(xgrids, chart.GridLine{
			Value: float64(i),
		})
	}

	// log scale the x axis
	for i, v := range x {
		if v == 1 {
			x[i] = float64(p)
		} else {
			x[i] = math.Log10(1 / (1 - v))
		}
	}

	gridStyle := chart.Style{
		Show:        true,
		StrokeWidth: 1,
		StrokeColor: chart.ColorBlack,
	}

	return &chart.Chart{
		XAxis: chart.XAxis{
			NameStyle:      chart.StyleShow(),
			Style:          chart.StyleShow(),
			Ticks:          xticks,
			TickStyle:      chart.StyleShow(),
			GridLines:      xgrids,
			GridMajorStyle: gridStyle,
		},
		YAxis: chart.YAxis{
			AxisType:       chart.YAxisSecondary,
			NameStyle:      chart.StyleShow(),
			Style:          chart.StyleShow(),
			GridMajorStyle: gridStyle,
			GridMinorStyle: gridStyle,
			ValueFormatter: func(x interface{}) string {
				return time.Duration(int64(x.(float64))).String()
			},
		},
		Series: []chart.Series{
			chart.ContinuousSeries{
				Style: chart.Style{
					Show:        true,
					StrokeColor: chart.GetDefaultColor(0),
					StrokeWidth: 2,
				},
				XValues: x,
				YValues: y,
			},
		},
	}
}
