package store

import (
	"errors"
	"fmt"
	"hash/maphash"
	"kvStore/config"
	"sync"
	"sync/atomic"
)

// ErrSnapshotAborted is returned by StoreSnapshot.ForEach when a concurrent
// Replace (or a newer BeginSnapshot) superseded the in-flight snapshot.
var ErrSnapshotAborted = errors.New("store: snapshot aborted")

// Number of hash shards for the in-memory map (must be a positive power of 2).
var shardCount = mustShardCount(config.Int("store.shards"))

func mustShardCount(n int) int {
	if n <= 0 || n&(n-1) != 0 {
		panic(fmt.Sprintf("config: store.shards must be a positive power of 2, got %d", n))
	}
	return n
}

type KVStore[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V)
	Delete(key K)
	Len() int
	Range(fn func(K, V) bool)
}

type beforeEntry[V any] struct {
	val     V
	existed bool
}

type shard[K ~string, V any] struct {
	mu      sync.RWMutex
	data    map[K]V
	snapGen uint64
	before  map[K]beforeEntry[V]
	// Pad to 128 bytes (48B fields + 80B pad) so adjacent shards don't share a
	// CPU cache line (64B on x86_64, 128B on Apple Silicon / arm64).
	_ [80]byte
}

type Store[K ~string, V any] struct {
	seed    maphash.Seed
	mask    uint64
	shards  []shard[K, V]
	snapSeq atomic.Uint64
}

// like a constructor
func NewStore[K ~string, V any]() *Store[K, V] {
	return newStoreWithShards[K, V](shardCount)
}

func newStoreWithShards[K ~string, V any](n int) *Store[K, V] {
	shards := make([]shard[K, V], n)
	for i := range shards {
		shards[i].data = make(map[K]V)
	}
	return &Store[K, V]{
		seed:   maphash.MakeSeed(),
		mask:   uint64(n - 1),
		shards: shards,
	}
}

func (s *Store[K, V]) shardIndex(key K) uint64 {
	return maphash.String(s.seed, string(key)) & s.mask
}

func (s *Store[K, V]) shardFor(key K) *shard[K, V] {
	return &s.shards[s.shardIndex(key)]
}

func (s *Store[K, V]) Get(key K) (V, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	v, ok := sh.data[key]
	sh.mu.RUnlock()
	return v, ok
}

// GetBytes looks up a byte-slice key without allocating a string copy.
func GetBytes[V any](s *Store[string, V], key []byte) (V, bool) {
	// maphash.Bytes produces the exact same hash as maphash.String.
	sh := &s.shards[maphash.Bytes(s.seed, key)&s.mask]
	sh.mu.RLock()
	v, ok := sh.data[string(key)]
	sh.mu.RUnlock()
	return v, ok
}

func (s *Store[K, V]) Set(key K, value V) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	if sh.snapGen != 0 {
		sh.recordBeforeLocked(key)
	}
	sh.data[key] = value
	sh.mu.Unlock()
}

func (s *Store[K, V]) Delete(key K) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	if sh.snapGen != 0 {
		sh.recordBeforeLocked(key)
	}
	delete(sh.data, key)
	sh.mu.Unlock()
}

func (sh *shard[K, V]) recordBeforeLocked(key K) {
	if _, recorded := sh.before[key]; recorded {
		return
	}
	if sh.before == nil {
		sh.before = make(map[K]beforeEntry[V])
	}
	old, existed := sh.data[key]
	sh.before[key] = beforeEntry[V]{val: old, existed: existed}
}

// Replace atomically swaps the store contents. The caller must not mutate data
// after passing it to Replace. Any in-flight snapshot is aborted.
func (s *Store[K, V]) Replace(data map[K]V) {
	// Build and populate the new shard maps before acquiring any locks so
	// concurrent readers/writers are only paused for the pointer swap.
	next := make([]map[K]V, len(s.shards))
	for i := range next {
		next[i] = make(map[K]V)
	}
	for k, v := range data {
		next[s.shardIndex(k)][k] = v
	}

	// Lock shards in fixed ascending order (0..N-1) to avoid deadlocks.
	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
	for i := range s.shards {
		s.shards[i].data = next[i]
		s.shards[i].snapGen = 0
		s.shards[i].before = nil
	}
	for i := range s.shards {
		s.shards[i].mu.Unlock()
	}
}

// StoreSnapshot holds a point-in-time consistent view of Store using
// per-shard before-image copy-on-write so reads and writes proceed without
// blocking on snapshot serialization.
type StoreSnapshot[K ~string, V any] struct {
	store *Store[K, V]
	gen   uint64
}

// BeginSnapshot marks a consistent point-in-time cut across all shards.
// Callers must call Close when finished (even if ForEach returns an error).
func (s *Store[K, V]) BeginSnapshot() *StoreSnapshot[K, V] {
	gen := s.snapSeq.Add(1)
	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
	for i := range s.shards {
		s.shards[i].snapGen = gen
		s.shards[i].before = nil
	}
	for i := range s.shards {
		s.shards[i].mu.Unlock()
	}
	return &StoreSnapshot[K, V]{store: s, gen: gen}
}

type kvPair[K ~string, V any] struct {
	k K
	v V
}

// ForEach visits every key-value pair present at BeginSnapshot time, one shard
// at a time. Concurrent reads are never blocked; each shard's copy-on-write
// tracking is released as soon as that shard is captured.
func (sn *StoreSnapshot[K, V]) ForEach(fn func(K, V) error) error {
	for i := range sn.store.shards {
		sh := &sn.store.shards[i]

		sh.mu.RLock()
		if sh.snapGen != sn.gen {
			sh.mu.RUnlock()
			return ErrSnapshotAborted
		}
		items := make([]kvPair[K, V], 0, len(sh.data)+len(sh.before))
		for k, v := range sh.data {
			if _, modified := sh.before[k]; !modified {
				items = append(items, kvPair[K, V]{k: k, v: v})
			}
		}
		for k, b := range sh.before {
			if b.existed {
				items = append(items, kvPair[K, V]{k: k, v: b.val})
			}
		}
		sh.mu.RUnlock()

		// Release CoW tracking on this shard immediately so subsequent writes
		// stop allocating before-image entries.
		sh.mu.Lock()
		if sh.snapGen == sn.gen {
			sh.snapGen = 0
			sh.before = nil
		}
		sh.mu.Unlock()

		for _, item := range items {
			if err := fn(item.k, item.v); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close releases any remaining per-shard copy-on-write state for this snapshot.
func (sn *StoreSnapshot[K, V]) Close() {
	if sn == nil || sn.store == nil {
		return
	}
	for i := range sn.store.shards {
		sh := &sn.store.shards[i]
		sh.mu.Lock()
		if sh.snapGen == sn.gen {
			sh.snapGen = 0
			sh.before = nil
		}
		sh.mu.Unlock()
	}
}

func (s *Store[K, V]) Len() int {
	total := 0
	for i := range s.shards {
		s.shards[i].mu.RLock()
		total += len(s.shards[i].data)
		s.shards[i].mu.RUnlock()
	}
	return total
}
