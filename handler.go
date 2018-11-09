package mon

import "net/http"

// Handler serves information about collected metrics.
const Handler = handler("")

type handler string

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

}
