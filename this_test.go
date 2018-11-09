package mon

import (
	"testing"

	"github.com/zeebo/assert"
)

type thisTest struct{}

func (t thisTest) method() string   { return This() }
func (t *thisTest) pmethod() string { return This() }

func TestThis(t *testing.T) {
	assert.Equal(t, This(),
		"github.com/zeebo/mon.TestThis")
	assert.Equal(t, thisTest{}.method(),
		"github.com/zeebo/mon.thisTest.method")
	assert.Equal(t, new(thisTest).pmethod(),
		"github.com/zeebo/mon.(*thisTest).pmethod")
}

func BenchmarkThis(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		This()
	}
}
