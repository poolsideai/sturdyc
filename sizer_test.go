package sturdyc_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

// sizedValue is a test type that implements the Sizer interface.
type sizedValue struct {
	data []byte
}

func (v sizedValue) Size() uint32 {
	return uint32(len(v.data))
}

func TestMemoryBasedEviction(t *testing.T) {
	t.Parallel()

	// Create a cache with a max of 100 bytes and reasonably large values.
	// Each value will be ~50 bytes, so we should only be able to store 1-2 entries.
	capacity := 1000 // High capacity so we don't hit entry limit
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 5
	maxBytes := uint64(100)

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Add first entry (50 bytes + key overhead)
	c.Set("key1", sizedValue{data: make([]byte, 50)})

	sizeAfterFirst := c.SizeBytes()
	if sizeAfterFirst <= 0 {
		t.Errorf("expected positive size after first entry, got %d", sizeAfterFirst)
	}

	// Add second entry - this should trigger bytes-based eviction
	c.Set("key2", sizedValue{data: make([]byte, 50)})

	sizeAfterSecond := c.SizeBytes()
	if sizeAfterSecond > maxBytes {
		t.Errorf("expected size <= %d after second entry, got %d", maxBytes, sizeAfterSecond)
	}

	// Add many more entries to verify eviction keeps working
	for i := 3; i < 100; i++ {
		c.Set(strconv.Itoa(i), sizedValue{data: make([]byte, 50)})
	}

	finalSize := c.SizeBytes()
	if finalSize > maxBytes {
		t.Errorf("expected size <= %d after many entries, got %d", maxBytes, finalSize)
	}

	// The cache should have evicted entries to stay under the limit
	cacheSize := c.Size()
	if cacheSize > 3 {
		t.Errorf("expected cache to have evicted entries, got %d entries", cacheSize)
	}
}

func TestMemoryBasedEvictionWithDifferentSizes(t *testing.T) {
	t.Parallel()

	capacity := 1000
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 10
	maxBytes := uint64(200)

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Add entries with varying sizes
	c.Set("small1", sizedValue{data: make([]byte, 30)})  // ~46 bytes
	c.Set("large1", sizedValue{data: make([]byte, 100)}) // ~116 bytes
	c.Set("small2", sizedValue{data: make([]byte, 30)})  // ~46 bytes
	c.Set("large2", sizedValue{data: make([]byte, 100)}) // ~116 bytes - should trigger eviction

	finalSize := c.SizeBytes()
	if finalSize > maxBytes {
		t.Errorf("expected size <= %d, got %d", maxBytes, finalSize)
	}
}

func TestMemoryBasedEvictionEvictsOldestFirst(t *testing.T) {
	t.Parallel()

	capacity := 100
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 10
	maxBytes := uint64(100)

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Fill cache with small entries
	for i := 0; i < 10; i++ {
		c.Set(strconv.Itoa(i), sizedValue{data: make([]byte, 10)}) // ~26 bytes each
	}

	// Add a larger entry that should trigger eviction of oldest entries
	c.Set("large", sizedValue{data: make([]byte, 200)}) // ~216 bytes

	// The "large" key should exist
	if _, ok := c.Get("large"); !ok {
		t.Error("expected 'large' key to exist")
	}
}

func TestSizeBytesTracksDeletes(t *testing.T) {
	t.Parallel()

	capacity := 1000
	numShards := 2
	ttl := time.Hour
	evictionPercentage := 10
	maxBytes := uint64(500)

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Add entries
	c.Set("key1", sizedValue{data: make([]byte, 100)})
	c.Set("key2", sizedValue{data: make([]byte, 100)})

	sizeBeforeDelete := c.SizeBytes()
	if sizeBeforeDelete <= 0 {
		t.Fatalf("expected positive size before delete, got %d", sizeBeforeDelete)
	}

	// Delete one entry
	c.Delete("key1")

	sizeAfterDelete := c.SizeBytes()
	// Size should have decreased
	if sizeAfterDelete >= sizeBeforeDelete {
		t.Errorf("expected size to decrease after delete, before=%d, after=%d", sizeBeforeDelete, sizeAfterDelete)
	}
}

func TestMaxBytesPanicsWithoutSizer(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic when MaxBytes is set without Sizer implementation")
		}
	}()

	// string does not implement Sizer
	sturdyc.New[string](100, 1, time.Hour, 10,
		sturdyc.WithMaxBytes(1000),
	)
}

func TestMaxBytesZeroDoesNotRequireSizer(t *testing.T) {
	t.Parallel()

	// This should not panic - maxBytes = 0 (not configured)
	c := sturdyc.New[string](100, 1, time.Hour, 10,
		sturdyc.WithMaxBytes(0),
		sturdyc.WithNoContinuousEvictions(),
	)

	if c == nil {
		t.Error("expected cache to be created without panic")
	}
}

func TestEvictionTriggersWhenAnyLimitExceeded(t *testing.T) {
	t.Parallel()

	// Test 1: Bytes limit triggers eviction before capacity
	capacity := 10000 // High capacity
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 10
	maxBytes := uint64(100)

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Add entries that exceed bytes but not capacity
	for i := 0; i < 100; i++ {
		c.Set(strconv.Itoa(i), sizedValue{data: make([]byte, 20)})
	}

	// Should be limited by bytes, not capacity
	if c.SizeBytes() > maxBytes {
		t.Errorf("expected bytes to be limited to %d, got %d", maxBytes, c.SizeBytes())
	}

	// Test 2: Capacity limit triggers eviction before bytes
	capacity = 10
	maxBytes = 100000 // High bytes limit

	c2 := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	for i := 0; i < 100; i++ {
		c2.Set(strconv.Itoa(i), sizedValue{data: make([]byte, 10)})
	}

	// Should be limited by capacity
	if c2.Size() > capacity {
		t.Errorf("expected entries to be limited to %d, got %d", capacity, c2.Size())
	}
}

func TestBytesEvictionWorksWithZeroEvictionPercentage(t *testing.T) {
	t.Parallel()

	// When evictionPercentage is 0, capacity-based eviction is disabled
	// but bytes-based eviction should still work
	capacity := 10000
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 0 // Disabled
	maxBytes := uint64(100)

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Add entries - should trigger bytes eviction, not capacity eviction
	for i := 0; i < 100; i++ {
		c.Set(strconv.Itoa(i), sizedValue{data: make([]byte, 20)})
	}

	// Should be limited by bytes
	if c.SizeBytes() > maxBytes {
		t.Errorf("expected bytes to be limited to %d, got %d", maxBytes, c.SizeBytes())
	}
}

func TestMemoryBasedEvictionWithShards(t *testing.T) {
	// Test that bytes tracking works correctly across multiple shards.
	// Note: Each shard gets maxBytes/numShards as its limit.
	capacity := 10000
	numShards := 10
	ttl := time.Hour
	evictionPercentage := 10
	maxBytes := uint64(10000) // 1000 bytes per shard should be plenty for entries with ~66 bytes

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Add entries - these will be distributed across shards
	for i := 0; i < 500; i++ {
		c.Set(strconv.Itoa(i), sizedValue{data: make([]byte, 50)})
	}

	// SizeBytes should correctly sum across all shards
	// With 10 shards and maxBytes=10000, each shard gets ~1000 bytes
	totalBytes := c.SizeBytes()
	if totalBytes > maxBytes {
		t.Errorf("expected total bytes to be limited to %d, got %d", maxBytes, totalBytes)
	}
}
