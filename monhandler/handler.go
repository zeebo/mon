package monhandler

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	chart "github.com/wcharczuk/go-chart"
	"github.com/zeebo/mon"
	"github.com/zeebo/mon/inthist"
)

// Handler serves information about collected metrics.
type Handler struct{}

func (Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/" || req.URL.Path == "" {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintln(w, `<meta charset="UTF-8">`)
		fmt.Fprintln(w, "<table border=1>")
		fmt.Fprintln(w, "<tr><td>name</td><td>total</td><td>sum</td><td>average</td><td>variance</td><td>stddev</td></tr>")
		mon.Times(func(name string, st *mon.State) bool {
			total := st.Total()
			sum, avg, vari := st.Variance()
			fmt.Fprintf(w, `<tr><td><a href="%[1]s">%[1]s</a></td><td>%d</td><td>%v</td><td>%v</td><td>%v</td><td>%v</td></tr>`,
				name, total, time.Duration(sum), time.Duration(avg), time.Duration(vari), time.Duration(math.Sqrt(vari)))
			return true
		})
		return
	}

	state := mon.LookupState(req.URL.Path[1:])
	if state == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	his := state.Histogram()

	width, height, pow := 1300, 300, -1
	if qpow, err := strconv.ParseInt(req.URL.Query().Get("pow"), 10, 0); err == nil {
		pow = int(qpow)
	}
	if qwidth, err := strconv.ParseInt(req.URL.Query().Get("width"), 10, 0); err == nil {
		width = int(qwidth)
	}
	if qheight, err := strconv.ParseInt(req.URL.Query().Get("height"), 10, 0); err == nil {
		height = int(qheight)
	}

	var buf bytes.Buffer
	_ = MakeChart(width, height, pow, his).Render(chart.SVG, &buf)

	w.Header().Set("Content-Type", chart.ContentTypeSVG)
	_, _ = w.Write(fixupViewbox(buf.Bytes(), width, height))
}

func fixupViewbox(data []byte, width, height int) []byte {
	parts := bytes.SplitN(data, []byte(">"), 2)
	if len(parts) != 2 {
		return data
	}
	return append(append(
		parts[0],
		fmt.Sprintf(` viewBox="-0.5 -0.5 %d %d">`, width, height)...),
		parts[1]...)
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

func MakeChart(width, height, pow int, hiss ...*inthist.Histogram) *chart.Chart {
	type line struct {
		x, y []float64
	}

	var lines []line
	var t float64

	largest := 1.0
	if pow > 0 {
		largest = 1.0 - math.Pow(0.1, float64(pow))
	}

	for _, his := range hiss {
		var l line
		his.Percentiles(func(value, count, total int64) {
			ptile := float64(count) / float64(total)
			if ptile <= largest {
				t = float64(total)
				l.x = append(l.x, ptile)
				l.y = append(l.y, float64(value))
			}
		})
		lines = append(lines, l)
	}

	// make a log axis
	if pow <= 0 {
		pow = 0
		for ti := t; ti >= 1; ti /= 10 {
			pow++
		}
	}

	var xticks []chart.Tick
	var xgrids []chart.GridLine
	for i := 0; i < pow; i++ {
		if i > 0 {
			xgrids = append(xgrids, chart.GridLine{Value: float64(i)})
		}
		xticks = append(xticks, chart.Tick{
			Value: float64(i),
			Label: getLabel(i),
		})
		if i == 0 {
			med := math.Log10(2)
			xgrids = append(xgrids, chart.GridLine{Value: med})
			xticks = append(xticks, chart.Tick{
				Value: med,
				Label: "50%",
			})

			quart := math.Log10(4)
			xgrids = append(xgrids, chart.GridLine{Value: quart})
			xticks = append(xticks, chart.Tick{
				Value: quart,
				Label: "75%",
			})
		}
	}

	// always add the largest grid line
	xgrids = append(xgrids, chart.GridLine{Value: float64(pow)})
	xticks = append(xticks, chart.Tick{
		Value: float64(pow),
		Label: getLabel(pow),
	})

	// log scale the x axis
	for _, l := range lines {
		for i, v := range l.x {
			if v == 1 {
				l.x[i] = float64(pow)
			} else {
				l.x[i] = math.Log10(1 / (1 - v))
			}
		}
	}

	gridStyle := chart.Style{
		Show:        true,
		StrokeWidth: 1,
		StrokeColor: chart.ColorBlack,
	}

	ch := &chart.Chart{
		Width:  width,
		Height: height,
		XAxis: chart.XAxis{
			NameStyle: chart.StyleShow(),
			Ticks:     xticks,
			GridLines: xgrids,

			Style:          chart.StyleShow(),
			TickStyle:      gridStyle,
			GridMajorStyle: gridStyle,
		},
		YAxis: chart.YAxis{
			AxisType:  chart.YAxisSecondary,
			NameStyle: chart.StyleShow(),
			ValueFormatter: func(x interface{}) string {
				return time.Duration(int64(x.(float64))).String()
			},

			Style:          chart.StyleShow(),
			TickStyle:      gridStyle,
			GridMinorStyle: gridStyle,
			GridMajorStyle: gridStyle,
		},
	}

	for i, l := range lines {
		ch.Series = append(ch.Series, chart.ContinuousSeries{
			Style: chart.Style{
				Show:        true,
				StrokeColor: chart.GetDefaultColor(i),
				StrokeWidth: 3,
			},
			XValues: l.x,
			YValues: l.y,
		})
	}

	return ch
}
