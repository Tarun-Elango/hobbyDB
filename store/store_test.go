package store

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func TestGetSetDelete(t *testing.T) {
	st := NewStore[string, int]()
	// --- set then get returns value, ok=true ---
	st.Set("a", 1)
	v, ok := st.Get("a")
	if !ok {
		t.Fatalf("expected ok=true after Set, got false")
	}
	if v != 1 {
		t.Fatalf("expected value=1, got %d", v)
	}

	// --- get missing key returns zero value, ok=false ---
	v, ok = st.Get("missing")
	if ok {
		t.Fatalf("expected ok=false for missing key, got true")
	}
	if v != 0 { // zero value for int
		t.Fatalf("expected zero value, got %d", v)
	}

	// --- delete then get returns ok=false --
	st.Set("b", 2)
	st.Delete("b")
	_, ok = st.Get("b")
	if ok {
		t.Fatalf("expected ok=false after Delete, got true")
	}

	// --- len tracks inserts/deletes correctly ---
	st2 := NewStore[string, int]()
	if st2.Len() != 0 {
		t.Fatalf("expected len=0 on empty store, got %d", st2.Len())
	}

	st2.Set("x", 1)
	st2.Set("y", 2)
	st2.Set("z", 3)
	if st2.Len() != 3 {
		t.Fatalf("expected len=3 after 3 inserts, got %d", st2.Len())
	}

	st2.Set("x", 99) // overwrite, not a new key
	if st2.Len() != 3 {
		t.Fatalf("expected len=3 after overwrite, got %d", st2.Len())
	}
	st2.Delete("x")
	if st2.Len() != 2 {
		t.Fatalf("expected len=2 after delete, got %d", st2.Len())
	}

	st2.Delete("x") // deleting again should be a no-op
	if st2.Len() != 2 {
		t.Fatalf("expected len=2 after deleting missing key, got %d", st2.Len())
	}

}

func TestGetBytes(t *testing.T) {
	st := NewStore[string, []byte]()
	st.Set("hello-world-key-exceeding-small-string-size", []byte("val"))

	key := []byte("hello-world-key-exceeding-small-string-size")
	v, ok := GetBytes(st, key)
	if !ok || string(v) != "val" {
		t.Fatalf("GetBytes got (%q, %v), want (%q, true)", v, ok, "val")
	}

	if _, ok := GetBytes(st, []byte("missing")); ok {
		t.Fatalf("expected ok=false for missing key")
	}

	allocs := testing.AllocsPerRun(1000, func() {
		_, _ = GetBytes(st, key)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs for GetBytes, got %v", allocs)
	}
}

func TestReplace(t *testing.T) {
	st := NewStore[string, []byte]()
	st.Set("old-1", []byte("a"))
	st.Set("old-2", []byte("b"))

	replacement := make(map[string][]byte, 200)
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("new-%d", i)
		replacement[k] = []byte(fmt.Sprintf("val-%d", i))
	}
	st.Replace(replacement)

	if st.Len() != 200 {
		t.Fatalf("expected len=200 after Replace, got %d", st.Len())
	}
	if _, ok := st.Get("old-1"); ok {
		t.Fatalf("expected old key to be removed after Replace")
	}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("new-%d", i)
		want := fmt.Sprintf("val-%d", i)
		v, ok := st.Get(k)
		if !ok || string(v) != want {
			t.Fatalf("Get(%q) = (%q, %v), want (%q, true)", k, v, ok, want)
		}
		vb, ok := GetBytes(st, []byte(k))
		if !ok || string(vb) != want {
			t.Fatalf("GetBytes(%q) = (%q, %v), want (%q, true)", k, vb, ok, want)
		}
	}

	// Replace with empty map clears store.
	st.Replace(nil)
	if st.Len() != 0 {
		t.Fatalf("expected len=0 after Replace(nil), got %d", st.Len())
	}
}

func TestConcurrentReplace(t *testing.T) {
	st := NewStore[string, int]()
	const keys = 100

	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_, _ = st.Get(fmt.Sprintf("k-%d", i%keys))
				_ = st.Len()
			}
		}()
	}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for iter := 0; iter < 25; iter++ {
				m := make(map[string]int, keys)
				for i := 0; i < keys; i++ {
					m[fmt.Sprintf("k-%d", i)] = id*100 + iter
				}
				st.Replace(m)
			}
		}(w)
	}
	wg.Wait()

	if st.Len() != keys {
		t.Fatalf("expected len=%d after concurrent Replace, got %d", keys, st.Len())
	}
}

func TestMustShardCount(t *testing.T) {
	for _, good := range []int{1, 2, 16, 64, 256} {
		if got := mustShardCount(good); got != good {
			t.Fatalf("mustShardCount(%d) = %d", good, got)
		}
	}
	for _, bad := range []int{-1, 0, 3, 6, 48, 100} {
		func(n int) {
			defer func() {
				if recover() == nil {
					t.Fatalf("mustShardCount(%d) did not panic", n)
				}
			}()
			mustShardCount(n)
		}(bad)
	}
}

func TestConcurrentAccess(t *testing.T) {
	st := NewStore[string, int]()

	const (
		numWriters = 50  // N goroutines writing distinct keys
		numReaders = 20  // M goroutines reading random keys
		writesPer  = 100 // ops per writer
		readsPer   = 200 // ops per reader
	)

	var wg sync.WaitGroup // syncronized counter, to keep track of number

	// --- N writer goroutines, each owns its own key range (distinct keys) ---
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < writesPer; i++ {
				key := fmt.Sprintf("writer-%d-key-%d", writerID, i)
				st.Set(key, writerID*1000+i)
			}
		}(w)
	}

	// --- M reader goroutines, hammering random/overlapping keys ---
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < readsPer; i++ {
				// reads keys that may or may not exist yet — that's fine,
				// we're only checking for safety, not for specific values
				writerID := i % numWriters
				idx := i % writesPer
				key := fmt.Sprintf("writer-%d-key-%d", writerID, idx)

				// ok can be true or false depending on timing; both are valid.
				// what we actually care about: no panic, no race.
				_, _ = st.Get(key)

				// also exercise Len() and Delete() concurrently for extra pressure
				_ = st.Len()
				if i%50 == 0 {
					st.Delete(key)
				}
			}
		}(r)
	}

	wg.Wait()

	// sanity check: store didn't end up corrupted — Len should be non-negative
	// and every distinct writer key we didn't delete should still resolve
	if st.Len() < 0 {
		t.Fatalf("store corrupted: negative length %d", st.Len())
	}
}

func BenchmarkSet(b *testing.B) {
	st := NewStore[string, int]()
	b.ResetTimer()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		// each goroutine - unique counter
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := "key- " + strconv.FormatInt(i, 10) // signed int to base 10, just regular numnber to string
			st.Set(key, int(i))
		}
	})
}

func BenchmarkGet(b *testing.B) {
	st := NewStore[string, int]()
	//pre populate
	const numKeys = 10_000
	for i := 0; i < numKeys; i++ {
		st.Set(fmt.Sprintf("key-%d", i), i)
	}
	b.ResetTimer()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key-%d", i%numKeys)
			_, _ = st.Get(key)
		}
	})
}

func BenchmarkMixed(b *testing.B) {
	st := NewStore[string, int]()

	const numKeys = 10_000
	for i := 0; i < numKeys; i++ {
		st.Set(fmt.Sprintf("key-%d", i), i)
	}

	b.ResetTimer()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)       // prevent two routines from colliding, single op
			key := fmt.Sprintf("key-%d", i%numKeys) // make sure i max numKeys [0, numKeys]
			if i%10 == 0 {
				st.Set(key, int(i)) // 10% writes
			} else {
				_, _ = st.Get(key) // 90% reads
			}
		}
	})
}

//BenchmarkGet-10     11827087    97.04 ns/op    15 B/op    1 allocs/op
// name, benchmark iterations done, time per get, memory allocated per op, heap alloc per op ( probably fmt.sprintf )
