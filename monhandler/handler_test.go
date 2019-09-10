package monhandler

import (
	"encoding/hex"
	"net/http"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mon"
)

func TestHandler(t *testing.T) {
	prebuilt := ""
	data, err := hex.DecodeString(prebuilt)
	assert.NoError(t, err)
	assert.NoError(t, mon.GetState("prebuilt").Histogram().Load(data))
	assert.NoError(t, http.ListenAndServe(":20000", new(Handler)))
}
