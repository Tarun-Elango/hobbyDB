// What this file checks:
// - Sanity-checks the linearizability checker (valid, stale, concurrent, flip-back, and unknown-write histories)
// - Sanity-checks stale-read counting on a known stale history

package correctness

import (
	"encoding/binary"
	"math"
	"sort"
	"testing"
)

// A tiny linearizability checker for one key treated as a register (the
// Wing & Gong search with memoisation, the same idea porcupine uses).
// Every SET writes a unique value, so a read names the write it saw.

const never = math.MaxInt64 // ret of a write whose outcome is unknown

type op struct {
	call, ret int64 // ns since test start; ret == never if the write never got an ack
	write     bool
	val       string // written value, or value read ("" = key missing)
}

type checkResult int

const (
	linearizable checkResult = iota
	notLinearizable
	gaveUp // search budget exceeded
)

func (r checkResult) String() string {
	return [...]string{"linearizable", "NOT linearizable", "inconclusive"}[r]
}

// checkRegister reports whether ops (all on one key, starting missing) can be
// ordered so each op takes effect between its call and return and every read
// returns the latest write. Unknown writes may take effect any time after
// their call, or never.
func checkRegister(ops []op, budget int) checkResult {
	sort.Slice(ops, func(i, j int) bool { return ops[i].call < ops[j].call })
	n := len(ops)
	done := make([]bool, n)
	words := make([]uint64, (n+63)/64)
	seen := map[string]bool{}
	required := 0
	for _, o := range ops {
		if o.ret != never {
			required++
		}
	}
	steps := 0

	var dfs func(state string, left int) checkResult
	dfs = func(state string, left int) checkResult {
		if left == 0 {
			return linearizable // only unknown writes left; they may never happen
		}
		if steps++; steps > budget {
			return gaveUp
		}
		buf := make([]byte, 0, len(words)*8+len(state))
		for _, w := range words {
			buf = binary.LittleEndian.AppendUint64(buf, w)
		}
		key := string(append(buf, state...))
		if seen[key] {
			return notLinearizable
		}
		seen[key] = true

		// Only ops that started before the earliest pending return may go next.
		minRet := int64(never)
		for i, o := range ops {
			if !done[i] && o.ret < minRet {
				minRet = o.ret
			}
		}
		res := notLinearizable
		for i, o := range ops {
			if done[i] || o.call > minRet {
				continue
			}
			if !o.write && o.val != state {
				continue
			}
			next := state
			if o.write {
				next = o.val
			}
			done[i] = true
			words[i/64] |= 1 << (i % 64)
			l := left
			if o.ret != never {
				l--
			}
			r := dfs(next, l)
			done[i] = false
			words[i/64] &^= 1 << (i % 64)
			if r == linearizable {
				return r
			}
			if r == gaveUp {
				res = gaveUp
			}
		}
		return res
	}
	return dfs("", required)
}

// staleReads counts reads that are provably stale: they returned the value
// of write w (or "missing") although some other acked write w2 started
// after w finished and finished before the read started.
func staleReads(ops []op) int {
	byVal := map[string]op{}
	var writes []op
	for _, o := range ops {
		if o.write {
			byVal[o.val] = o
			if o.ret != never {
				writes = append(writes, o)
			}
		}
	}
	stale := 0
	for _, r := range ops {
		if r.write {
			continue
		}
		w, known := byVal[r.val]
		for _, w2 := range writes {
			if w2.ret >= r.call {
				continue
			}
			if r.val == "" || (known && w2.call > w.ret) {
				stale++
				break
			}
		}
	}
	return stale
}

// TestChecker sanity-checks the checker itself on hand-made histories.
func TestChecker(t *testing.T) {
	cases := []struct {
		name string
		ops  []op
		want checkResult
	}{
		{"read after write", []op{{0, 1, true, "a"}, {2, 3, false, "a"}}, linearizable},
		{"stale read", []op{{0, 1, true, "a"}, {2, 3, true, "b"}, {4, 5, false, "a"}}, notLinearizable},
		{"concurrent read sees either", []op{{0, 10, true, "a"}, {1, 2, false, ""}, {3, 4, false, "a"}}, linearizable},
		{"value flips back", []op{{0, 10, true, "a"}, {1, 2, false, "a"}, {3, 4, false, ""}}, notLinearizable},
		{"unknown write may apply", []op{{0, never, true, "a"}, {5, 6, false, "a"}}, linearizable},
		{"unknown write may not apply", []op{{0, never, true, "a"}, {5, 6, false, ""}}, linearizable},
	}
	for _, tc := range cases {
		if got := checkRegister(tc.ops, 1e6); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
	if n := staleReads([]op{{0, 1, true, "a"}, {2, 3, true, "b"}, {4, 5, false, "a"}}); n != 1 {
		t.Errorf("staleReads = %d, want 1", n)
	}
}
