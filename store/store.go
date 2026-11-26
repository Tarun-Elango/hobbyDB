package store

import (
	"fmt"
	"hash/maphash"
	"kvStore/config"
	"sync"
)

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

type shard[K ~string, V any] struct {
	mu   sync.RWMutex
	data map[K]V
	// Pad to 128 bytes (32B fields + 96B pad) so adjacent shards don't share a
	// CPU cache line (64B on x86_64, 128B on Apple Silicon / arm64).
	_ [96]byte
}

type Store[K ~string, V any] struct {
	seed   maphash.Seed
	mask   uint64
	shards []shard[K, V]
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
	sh.data[key] = value
	sh.mu.Unlock()
}

func (s *Store[K, V]) Delete(key K) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	delete(sh.data, key)
	sh.mu.Unlock()
}

// Replace atomically swaps the store contents. The caller must not mutate data
// after passing it to Replace.
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
	}
	for i := range s.shards {
		s.shards[i].mu.Unlock()
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
