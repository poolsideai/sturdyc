package sturdyc

import (
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// cost of string header in bytes
	_stringHeaderBytes = uint32(unsafe.Sizeof(""))
	// entry type wrapper overhead
	_entryStructBytes = uint32(unsafe.Sizeof(entry[any]{}))
)

// entry represents a single cache entry.
type entry[T any] struct {
	key                  string
	value                T
	expiresAt            time.Time
	backgroundRefreshAt  time.Time
	synchronousRefreshAt time.Time
	numOfRefreshRetries  int
	isMissingRecord      bool
	// Memory footprint of this entry in bytes.
	size uint32
	// SIEVE eviction fields.
	visited atomic.Bool
	prev    *entry[T]
	next    *entry[T]
}

// shard is a thread-safe data structure that holds a subset of the cache entries.
type shard[T any] struct {
	sync.RWMutex
	*Config
	capacity int
	// capacityInBytes is the capacity of the shard in bytes. It is set to a value greater than 0 when
	// cache configuration defines MaxBytes > 0.
	capacityInBytes    uint64
	ttl                time.Duration
	entries            map[string]*entry[T]
	evictionPercentage int
	// Total memory currently in use by this shard in bytes.
	currentBytes uint64
	// SIEVE eviction: doubly-linked list and hand pointer.
	head *entry[T]
	tail *entry[T]
	hand *entry[T]
}

// newShard creates a new shard and returns a pointer to it.
func newShard[T any](capacity int, ttl time.Duration, evictionPercentage int, cfg *Config, capacityInBytes uint64) *shard[T] {
	return &shard[T]{
		Config:             cfg,
		capacity:           capacity,
		ttl:                ttl,
		entries:            make(map[string]*entry[T]),
		evictionPercentage: evictionPercentage,
		currentBytes:       0,
		capacityInBytes:    capacityInBytes,
	}
}

// calculateEntrySize computes the memory footprint of a key-value pair.
// If MaxBytes is configured, the value is expected to implement Sizer.
func (s *shard[T]) calculateEntrySize(key string, value T) uint32 {
	keySize := _stringHeaderBytes + uint32(len(key))

	var valueSize uint32
	if sizer, ok := any(value).(Sizer); ok {
		valueSize = sizer.Size()
	}

	// Add overhead for the entry struct and map entry.
	// This is an approximation - Go's map entries have internal overhead.
	return keySize + valueSize + 16 + _entryStructBytes
}

// size returns the number of entries in the shard.
func (s *shard[T]) size() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.entries)
}

// pushFront inserts an entry at the head of the SIEVE linked list.
// Should be called with a write lock held.
func (s *shard[T]) pushFront(e *entry[T]) {
	e.prev = nil
	e.next = s.head
	if s.head != nil {
		s.head.prev = e
	}
	s.head = e
	if s.tail == nil {
		s.tail = e
	}
}

// unlink removes an entry from the SIEVE linked list.
// Should be called with a write lock held.
func (s *shard[T]) unlink(e *entry[T]) {
	if s.hand == e {
		s.hand = e.prev
	}
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		s.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		s.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

// evictExpired evicts all the expired entries in the shard.
func (s *shard[T]) evictExpired() {
	s.Lock()
	defer s.Unlock()

	var entriesEvicted int
	for _, e := range s.entries {
		if s.clock.Now().After(e.expiresAt) {
			s.currentBytes -= uint64(e.size)
			if s.useSIEVE {
				s.unlink(e)
			}
			delete(s.entries, e.key)
			entriesEvicted++
		}
	}
	s.reportEntriesEvicted(entriesEvicted)
}

// sieveEvict runs the SIEVE eviction algorithm, evicting entries until
// shouldStop returns true or the shard is empty. Returns the number of
// entries evicted. Should be called with a lock.
func (s *shard[T]) sieveEvict(shouldStop func() bool) int {
	entriesEvicted := 0
	for !shouldStop() && len(s.entries) > 0 {
		if s.hand == nil {
			s.hand = s.tail
		}
		if s.hand == nil {
			break
		}
		if s.hand.visited.Load() {
			s.hand.visited.Store(false)
			s.hand = s.hand.prev
		} else {
			victim := s.hand
			s.hand = s.hand.prev
			s.unlink(victim)
			s.currentBytes -= uint64(victim.size)
			delete(s.entries, victim.key)
			entriesEvicted++
		}
	}
	return entriesEvicted
}

// forceEvict evicts a certain percentage of the entries in the shard.
// Uses SIEVE when enabled, otherwise falls back to TTL-based eviction.
// Should be called with a lock.
func (s *shard[T]) forceEvict() {
	s.reportForcedEviction()

	// Check if we should evict all entries.
	if s.evictionPercentage == 100 {
		evictedCount := len(s.entries)
		s.entries = make(map[string]*entry[T])
		s.currentBytes = 0
		s.head = nil
		s.tail = nil
		s.hand = nil
		s.reportEntriesEvicted(evictedCount)
		return
	}

	if s.useSIEVE {
		entriesToEvict := int(float64(len(s.entries)) * float64(s.evictionPercentage) / 100)
		sizeBefore := len(s.entries)
		s.sieveEvict(func() bool {
			return sizeBefore-len(s.entries) >= entriesToEvict
		})
		s.reportEntriesEvicted(sizeBefore - len(s.entries))
		return
	}

	// TTL-based eviction: evict entries with the oldest expiration times.
	expirationTimes := make([]time.Time, 0, len(s.entries))
	for _, e := range s.entries {
		expirationTimes = append(expirationTimes, e.expiresAt)
	}

	// We could have a lumpy distribution of expiration times. As an example, we
	// might have 100 entries in the cache but only 2 unique expiration times. In
	// order to not over-evict when trying to remove 10%, we'll have to keep
	// track of the number of entries that we've evicted.
	percentage := float64(s.evictionPercentage) / 100
	cutoff := FindCutoff(expirationTimes, percentage)
	entriesToEvict := int(float64(len(expirationTimes)) * percentage)
	entriesEvicted := 0
	for key, e := range s.entries {
		// Here we're essentially saying: if e.expiresAt <= cutoff.
		if !e.expiresAt.After(cutoff) {
			s.currentBytes -= uint64(e.size)
			delete(s.entries, key)
			entriesEvicted++

			if entriesEvicted == entriesToEvict {
				break
			}
		}
	}
	s.reportEntriesEvicted(entriesEvicted)
}

// forceEvictBytes evicts entries until there's enough room for the new entry.
// Uses SIEVE when enabled, otherwise falls back to TTL-based eviction.
// Should be called with a lock.
func (s *shard[T]) forceEvictBytes(newEntrySize uint32) {
	s.reportForcedEviction()

	if s.useSIEVE {
		n := s.sieveEvict(func() bool {
			return s.currentBytes+uint64(newEntrySize) <= s.capacityInBytes
		})
		s.reportEntriesEvicted(n)
		return
	}

	// TTL-based eviction: sort entries by expiration and evict oldest first.
	type candidate struct {
		key  string
		exp  time.Time
		size uint32
	}
	candidates := make([]candidate, 0, len(s.entries))
	for key, e := range s.entries {
		candidates = append(candidates, candidate{key, e.expiresAt, e.size})
	}
	slices.SortFunc(candidates, func(a, b candidate) int {
		return a.exp.Compare(b.exp)
	})
	entriesEvicted := 0
	for _, c := range candidates {
		if s.currentBytes+uint64(newEntrySize) <= s.capacityInBytes {
			break
		}
		s.currentBytes -= uint64(c.size)
		delete(s.entries, c.key)
		entriesEvicted++
	}
	s.reportEntriesEvicted(entriesEvicted)
}

// get attempts to retrieve a value from the shard.
//
// Parameters:
//
//	key: The key for which the value is to be retrieved.
//
// Returns:
//
//	val: The value associated with the key, if it exists.
//	exists: A boolean indicating if the value exists in the shard.
//	markedAsMissing: A boolean indicating if the key has been marked as a missing record.
//	refresh: A boolean indicating if the value should be refreshed in the background.
func (s *shard[T]) get(key string) (val T, exists, markedAsMissing, backgroundRefresh, synchronousRefresh bool) {
	s.RLock()
	item, ok := s.entries[key]
	if !ok {
		s.RUnlock()
		return val, false, false, false, false
	}

	if s.clock.Now().After(item.expiresAt) {
		s.RUnlock()
		return val, false, false, false, false
	}

	// Mark the entry as visited for SIEVE eviction.
	if s.useSIEVE {
		item.visited.Store(true)
	}

	// Check if the record should be synchronously refreshed.
	if s.earlyRefreshes && s.clock.Now().After(item.synchronousRefreshAt) {
		s.RUnlock()
		return item.value, true, item.isMissingRecord, false, true
	}

	shouldRefresh := s.earlyRefreshes && s.clock.Now().After(item.backgroundRefreshAt)
	if shouldRefresh {
		// Release the read lock, and switch to a write lock.
		s.RUnlock()
		s.Lock()

		// However, during the time it takes to switch locks, another goroutine
		// might have acquired it and moved the refreshAt. Therefore, we'll have to
		// check if this operation should still be performed.
		if !s.clock.Now().After(item.backgroundRefreshAt) {
			s.Unlock()
			return item.value, true, item.isMissingRecord, false, false
		}

		// Update the "refreshAt" so no other goroutines attempts to refresh the same entry.
		nextRefresh := s.retryBaseDelay * (1 << item.numOfRefreshRetries)
		item.backgroundRefreshAt = s.clock.Now().Add(nextRefresh)
		item.numOfRefreshRetries++

		s.Unlock()
		return item.value, true, item.isMissingRecord, shouldRefresh, false
	}

	s.RUnlock()
	return item.value, true, item.isMissingRecord, false, false
}

// set writes a key-value pair to the shard and returns a
// boolean indicating whether an eviction was performed.
func (s *shard[T]) set(key string, value T, isMissingRecord bool) bool {
	s.Lock()
	defer s.Unlock()

	entrySize := s.calculateEntrySize(key, value)

	// Check if we need to perform eviction based on capacity or bytes.
	atCapacity := len(s.entries) >= s.capacity
	overBytes := s.capacityInBytes > 0 && s.currentBytes+uint64(entrySize) > s.capacityInBytes

	// Check for an existing entry before eviction.
	existingEntry, isUpdate := s.entries[key]

	// If we're over the bytes limit, we must evict (bytes eviction is independent of evictionPercentage)
	if overBytes {
		s.forceEvictBytes(entrySize)
		// Fall through to create the entry after eviction
	} else if atCapacity && !isUpdate {
		// If we're at capacity and eviction is enabled, evict based on capacity.
		// Updates don't increase entry count, so they don't need capacity eviction.
		// If the cache is configured to not evict any entries, return early.
		if s.evictionPercentage < 1 {
			return false
		}
		s.forceEvict()
	}

	if isUpdate {
		s.currentBytes -= uint64(existingEntry.size)
	}

	now := s.clock.Now()
	newEntry := &entry[T]{
		key:             key,
		value:           value,
		expiresAt:       now.Add(s.ttl),
		isMissingRecord: isMissingRecord,
		size:            entrySize,
	}

	if s.earlyRefreshes {
		// If there is a difference between the min- and maxRefreshTime we'll use that to
		// set a random padding so that the refreshes get spread out evenly over time.
		var padding time.Duration
		if s.minAsyncRefreshTime != s.maxAsyncRefreshTime {
			padding = time.Duration(rand.Int64N(int64(s.maxAsyncRefreshTime - s.minAsyncRefreshTime)))
		}
		newEntry.backgroundRefreshAt = now.Add(s.minAsyncRefreshTime + padding)
		newEntry.synchronousRefreshAt = now.Add(s.syncRefreshTime)
		newEntry.numOfRefreshRetries = 0
	}

	if s.useSIEVE {
		if isUpdate {
			// For updates, replace the entry in its current list position to preserve SIEVE ordering.
			newEntry.prev = existingEntry.prev
			newEntry.next = existingEntry.next
			if existingEntry.prev != nil {
				existingEntry.prev.next = newEntry
			} else {
				s.head = newEntry
			}
			if existingEntry.next != nil {
				existingEntry.next.prev = newEntry
			} else {
				s.tail = newEntry
			}
			if s.hand == existingEntry {
				s.hand = newEntry
			}
		} else {
			// New entries are inserted at the head of the list.
			s.pushFront(newEntry)
		}
	}

	s.entries[key] = newEntry
	s.currentBytes += uint64(entrySize)
	return atCapacity || overBytes
}

// delete removes a key from the shard.
func (s *shard[T]) delete(key string) {
	s.Lock()
	defer s.Unlock()
	if entry, exists := s.entries[key]; exists {
		s.currentBytes -= uint64(entry.size)
		if s.useSIEVE {
			s.unlink(entry)
		}
		delete(s.entries, key)
	}
}

// keys returns all non-expired keys in the shard.
func (s *shard[T]) keys() []string {
	s.RLock()
	defer s.RUnlock()
	keys := make([]string, 0, len(s.entries))
	for k, v := range s.entries {
		if s.clock.Now().After(v.expiresAt) {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}
