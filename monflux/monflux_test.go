package monflux

import (
	"bytes"
	"errors"
	"testing"

	"github.com/zeebo/mon"
)

func TestMetrics(t *testing.T) {
	for i := 0; i < 100000; i++ {
		func() {
			var err error
			defer mon.Start().Stop(&err)
			err = errors.New("problem")
		}()
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		defer mon.Start().Stop(nil)
		done <- struct{}{}
		<-done
	}()
	<-done

	var buf bytes.Buffer
	err := Write(&buf)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("\n%s", buf.String())
}
