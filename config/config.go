// Package config loads settings.properties. There are no built-in defaults:
// a missing file, a missing key, or a malformed value panics at startup.
package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const fileName = "settings.properties"

var props = mustLoad()

// Int returns the integer value of key.
func Int(key string) int {
	n, err := strconv.Atoi(raw(key))
	if err != nil {
		panic(fmt.Sprintf("config: %s: %v", key, err))
	}
	return n
}

// Duration returns key interpreted as milliseconds.
func Duration(key string) time.Duration {
	return time.Duration(Int(key)) * time.Millisecond
}

func raw(key string) string {
	v, ok := props[key]
	if !ok {
		panic(fmt.Sprintf("config: missing key %q in %s", key, fileName))
	}
	return v
}

func mustLoad() map[string]string {
	path := locate()
	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("config: %v", err))
	}
	defer f.Close()

	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			panic(fmt.Sprintf("config: %s: bad line %q", path, line))
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	if err := sc.Err(); err != nil {
		panic(fmt.Sprintf("config: %v", err))
	}
	return out
}

// locate uses $KVSTORE_SETTINGS if set, otherwise walks up from the working
// directory (then the executable's directory) looking for settings.properties.
func locate() string {
	if p := os.Getenv("KVSTORE_SETTINGS"); p != "" {
		return p
	}
	var starts []string
	if wd, err := os.Getwd(); err == nil {
		starts = append(starts, wd)
	}
	if exe, err := os.Executable(); err == nil {
		starts = append(starts, filepath.Dir(exe))
	}
	for _, dir := range starts {
		for {
			p := filepath.Join(dir, fileName)
			if _, err := os.Stat(p); err == nil {
				return p
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	panic("config: " + fileName + " not found (set KVSTORE_SETTINGS)")
}
