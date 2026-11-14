// Package logx is a tiny switchable logger for server-side debug output.
// Output is off by default; cmd/server enables it with the -verbose flag.
package logx

import (
	"fmt"
	"os"
	"sync/atomic"
)

var enabled atomic.Bool

// SetEnabled turns server logging on or off.
func SetEnabled(on bool) { enabled.Store(on) }

// Enabled reports whether logging is on.
func Enabled() bool { return enabled.Load() }

// Printf prints to stdout when logging is enabled.
func Printf(format string, args ...any) {
	if enabled.Load() {
		fmt.Printf(format, args...)
	}
}

// Errorf prints to stderr when logging is enabled.
func Errorf(format string, args ...any) {
	if enabled.Load() {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}
