package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestStoreSnapshotBeforeImageConsistency(t *testing.T) {
	s := NewStore[string, []byte]()
	for i := 0; i < 200; i++ {
		s.Set(fmt.Sprintf("k-%03d", i), []byte(fmt.Sprintf("v0-%03d", i)))
	}

	snap := s.BeginSnapshot()
	defer snap.Close()

	// Mutate live store heavily while snapshot is open:
	// - overwrite existing keys multiple times
	// - delete existing keys, then reinsert, then delete again
	// - insert brand-new keys and delete some of them
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("k-%03d", i)
		s.Set(k, []byte("v1"))
		s.Set(k, []byte("v2"))
	}
	for i := 50; i < 100; i++ {
		k := fmt.Sprintf("k-%03d", i)
		s.Delete(k)
		s.Set(k, []byte("resurrected"))
		s.Delete(k)
	}
	for i := 200; i < 260; i++ {
		k := fmt.Sprintf("k-%03d", i)
		s.Set(k, []byte("new"))
		if i%2 == 0 {
			s.Delete(k)
		}
	}

	got := make(map[string]string)
	if err := snap.ForEach(func(k string, v []byte) error {
		got[k] = string(v)
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}

	if len(got) != 200 {
		t.Fatalf("snapshot len = %d, want 200", len(got))
	}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("k-%03d", i)
		want := fmt.Sprintf("v0-%03d", i)
		if got[k] != want {
			t.Fatalf("snapshot[%s] = %q, want %q", k, got[k], want)
		}
	}

	// Verify live store kept all post-snapshot mutations.
	if v, ok := s.Get("k-000"); !ok || string(v) != "v2" {
		t.Fatalf("live k-000 = %q (%v), want v2", v, ok)
	}
	if _, ok := s.Get("k-050"); ok {
		t.Fatal("live k-050 should be deleted")
	}
	if v, ok := s.Get("k-201"); !ok || string(v) != "new" {
		t.Fatalf("live k-201 = %q (%v), want new", v, ok)
	}
}

func TestStoreSnapshotAbortedByReplace(t *testing.T) {
	s := NewStore[string, []byte]()
	s.Set("a", []byte("1"))
	snap := s.BeginSnapshot()
	defer snap.Close()

	s.Replace(map[string][]byte{"b": []byte("2")})

	err := snap.ForEach(func(string, []byte) error { return nil })
	if !errors.Is(err, ErrSnapshotAborted) {
		t.Fatalf("ForEach err = %v, want ErrSnapshotAborted", err)
	}
}

func TestStoreSnapshotConcurrentReadsAndWrites(t *testing.T) {
	s := NewStore[string, []byte]()
	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("k-%d", i), []byte("init"))
	}

	snap := s.BeginSnapshot()
	defer snap.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			s.Set(fmt.Sprintf("k-%d", i), []byte("mutated"))
			s.Set(fmt.Sprintf("extra-%d", i), []byte("extra"))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _ = s.Get(fmt.Sprintf("k-%d", i))
		}
	}()

	got := make(map[string]string)
	if err := snap.ForEach(func(k string, v []byte) error {
		got[k] = string(v)
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	wg.Wait()

	if len(got) != 100 {
		t.Fatalf("snapshot len = %d, want 100", len(got))
	}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k-%d", i)
		if got[k] != "init" {
			t.Fatalf("snapshot[%s] = %q, want init", k, got[k])
		}
	}
}
