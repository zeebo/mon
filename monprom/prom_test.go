package monprom

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/zeebo/mon"
)

func TestMetrics(t *testing.T) {
	for i := 0; i < 1000; i++ {
		func() { defer mon.Start().Stop() }()
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		defer mon.Start().Stop()
		done <- struct{}{}
		<-done
	}()
	<-done

	reg := prometheus.NewRegistry()
	reg.Register(Collector{ExcludeHistograms: true})

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	for _, mf := range mfs {
		if _, err := expfmt.MetricFamilyToText(&buf, mf); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("\n%s", buf.String())
}
