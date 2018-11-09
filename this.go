package mon

import (
	"runtime"
	"strings"
)

// This returns the package/function name being called.
func This() string {
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		return "unknown"
	}
	return strings.TrimSuffix(runtime.FuncForPC(pc).Name(), ".init")
}

// this2 is here because having both dispatch to some generic thisN
// actually doesn't inline. double function calls. the horror.
func this2() string {
	pc, _, _, ok := runtime.Caller(2)
	if !ok {
		return "unknown"
	}
	return strings.TrimSuffix(runtime.FuncForPC(pc).Name(), ".init")
}
