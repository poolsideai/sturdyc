package sturdyc_test

import (
	"runtime"
	"strconv"
	"strings"
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
	// Create a cache with a max of 300 bytes and reasonably large values.
	// Each value will be ~200 bytes, so we should only be able to store 1 entry.
	capacity := 1000 // High capacity so we don't hit entry limit
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 5
	maxBytes := uint64(300)

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Add first entry (~200 bytes)
	c.Set("key1", sizedValue{data: make([]byte, 50)})

	sizeAfterFirst := c.SizeBytes()
	assertThat(t, sizeAfterFirst > 0, "expected positive size after first entry, got %d", sizeAfterFirst)

	// Add second entry - this should trigger bytes-based eviction
	c.Set("key2", sizedValue{data: make([]byte, 50)})

	sizeAfterSecond := c.SizeBytes()
	assertThat(t, sizeAfterSecond <= maxBytes, "expected size <= %d after second entry, got %d", maxBytes, sizeAfterSecond)

	// Add many more entries to verify eviction keeps working
	for i := 3; i < 100; i++ {
		c.Set(strconv.Itoa(i), sizedValue{data: make([]byte, 50)})
	}

	finalSize := c.SizeBytes()
	assertThat(t, finalSize <= maxBytes, "expected size <= %d after many entries, got %d", maxBytes, finalSize)

	// The cache should have evicted entries to stay under the limit
	cacheSize := c.Size()
	assertThat(t, cacheSize <= 3, "expected cache to have evicted entries, got %d entries", cacheSize)
}

func TestMemoryBasedEvictionWithDifferentSizes(t *testing.T) {
	t.Parallel()

	capacity := 1000
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 10
	maxBytes := uint64(300)

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
	assertThat(t, finalSize <= maxBytes, "expected size <= %d, got %d", maxBytes, finalSize)
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
	_, ok := c.Get("large")
	assertThat(t, ok, "expected 'large' key to exist")
}

func TestSizeBytesTracksDeletes(t *testing.T) {
	// configure cache so that size-based eviction won't be triggered
	capacity := 1000
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 10
	maxBytes := uint64(10000)

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Add entries
	c.Set("key1", sizedValue{data: make([]byte, 100)})
	c.Set("key2", sizedValue{data: make([]byte, 100)})

	sizeBeforeDelete := c.SizeBytes()
	assertThat(t, sizeBeforeDelete > 0, "expected positive size before delete, got %d", sizeBeforeDelete)

	// Delete one entry
	c.Delete("key1")

	sizeAfterDelete := c.SizeBytes()
	// Size should have decreased
	assertThat(t, sizeAfterDelete < sizeBeforeDelete, "expected size to decrease after delete, before=%d, after=%d", sizeBeforeDelete, sizeAfterDelete)
}

func TestMaxBytesPanicsWithoutSizer(t *testing.T) {
	assertPanic(t, func() {
		// string does not implement Sizer
		sturdyc.New[string](100, 1, time.Hour, 10,
			sturdyc.WithMaxBytes(1000),
		)
	}, "expected panic when MaxBytes is set without Sizer implementation")
}

func TestMaxBytesZeroDoesNotRequireSizer(t *testing.T) {
	// This should not panic - maxBytes = 0 (not configured)
	c := sturdyc.New[string](100, 1, time.Hour, 10,
		sturdyc.WithMaxBytes(0),
		sturdyc.WithNoContinuousEvictions(),
	)

	assertThat(t, c != nil, "expected cache to be created without panic")
}

func TestEvictionTriggersWhenAnyLimitExceeded(t *testing.T) {
	// Test 1: Bytes limit triggers eviction before capacity
	capacity := 10000 // High capacity
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 10
	maxBytes := uint64(1000)

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Add entries that exceed bytes but not capacity
	for i := 0; i < 100; i++ {
		c.Set(strconv.Itoa(i), sizedValue{data: make([]byte, 20)})
	}

	// Should be limited by bytes, not capacity
	assertThat(t, c.SizeBytes() <= maxBytes, "expected bytes to be limited to %d, got %d", maxBytes, c.SizeBytes())

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
	assertThat(t, c2.Size() <= capacity, "expected entries to be limited to %d, got %d", capacity, c2.Size())
}

func TestBytesEvictionWorksWithZeroEvictionPercentage(t *testing.T) {
	// When evictionPercentage is 0, capacity-based eviction is disabled
	// but bytes-based eviction should still work
	capacity := 10000
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 0 // Disabled
	maxBytes := uint64(1000)

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Add entries - should trigger bytes eviction, not capacity eviction
	for i := 0; i < 100; i++ {
		c.Set(strconv.Itoa(i), sizedValue{data: make([]byte, 20)})
	}

	// Should be limited by bytes
	assertThat(t, c.SizeBytes() <= maxBytes, "expected bytes to be limited to %d, got %d", maxBytes, c.SizeBytes())
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
	assertThat(t, totalBytes <= maxBytes, "expected total bytes to be limited to %d, got %d", maxBytes, totalBytes)
}

// TestCalculateEntrySizeAccuracy tests that the calculated entry size
// is reasonably accurate when compared to actual heap allocations
// reported by the Go runtime.
func TestCalculateEntrySizeAccuracy(t *testing.T) {
	// Force garbage collection and get baseline heap stats
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Create a cache with a single shard and Sizer implementation
	capacity := 10000
	numShards := 1
	ttl := time.Hour
	evictionPercentage := 10
	maxBytes := uint64(1 << 30) // Very large to avoid eviction during test

	c := sturdyc.New[sizedValue](capacity, numShards, ttl, evictionPercentage,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(maxBytes),
	)

	// Number of entries to add for meaningful measurement
	numEntries := 1000
	valueSize := 100 // Each value will have 100 bytes of data

	for i := 0; i < numEntries; i++ {
		c.Set(strconv.Itoa(i), sizedValue{data: make([]byte, valueSize)})
	}

	// Allow GC to settle and get final heap stats
	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	calculatedSize := c.SizeBytes()
	heapGrowth := m2.Alloc - m1.Alloc

	t.Logf("Calculated size: %d bytes", calculatedSize)
	t.Logf("Heap growth: %d bytes, delta: %d, percentage: %.2f%%", heapGrowth, heapGrowth-calculatedSize,
		float64(heapGrowth-calculatedSize)/float64(calculatedSize)*100)

	// The calculated size should be within a reasonable range of the actual heap growth.
	// Due to Go's memory allocator behavior (rounding, fragmentation, GC overhead),
	// we allow for some variance but the calculated size should be close.
	// Map entries have significant overhead in Go (hash map internal structures)
	// so we expect calculated < actual.
	minExpected := calculatedSize
	maxExpected := uint64(float64(calculatedSize) * 1.5) // Allow up to 50% more for map overhead

	assertThat(t, heapGrowth >= minExpected, "heap growth (%d) is less than calculated size (%d)", heapGrowth, calculatedSize)
	assertThat(t, heapGrowth <= maxExpected, "heap growth (%d) exceeds expected maximum (%d), possibly due to allocator overhead", heapGrowth, maxExpected)
	assertThat(t, calculatedSize > 0, "expected non-zero calculated size")
}

// TestCalculateEntrySizeMultipleEntries verifies that the size calculation
// scales correctly with multiple entries.
func TestCalculateEntrySizeMultipleEntries(t *testing.T) {
	t.Parallel()

	c := sturdyc.New[testSizerValue](10000, 1, time.Hour, 10,
		sturdyc.WithNoContinuousEvictions(),
		sturdyc.WithMaxBytes(1<<30),
	)

	numEntries := 100
	valueSize := uint32(50)

	for i := 0; i < numEntries; i++ {
		c.Set(strconv.Itoa(i), testSizerValue{data: make([]byte, valueSize)})
	}

	actualSize := c.SizeBytes()

	// Each entry contributes at least valueSize bytes
	minSize := uint64(numEntries) * uint64(valueSize)
	assertThat(t, actualSize >= minSize, "size %d is less than minimum expected %d", actualSize, minSize)

	// Each entry contributes at most valueSize + overhead
	maxSize := uint64(numEntries) * (uint64(valueSize) + 200)
	assertThat(t, actualSize <= uint64(maxSize), "size %d exceeds maximum expected %d", actualSize, maxSize)

	t.Logf("Size for %d entries with %d byte values: %d bytes", numEntries, valueSize, actualSize)
}

// TestCalculateEntrySizeFormula verifies that the size calculation
// produces correct values for known inputs.
func TestCalculateEntrySizeFormula(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		keyLen    int
		valueSize uint32
	}{
		{name: "small_key_small_value", keyLen: 5, valueSize: 10},
		{name: "medium_key_medium_value", keyLen: 20, valueSize: 100},
		{name: "large_key_large_value", keyLen: 100, valueSize: 1000},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := sturdyc.New[testSizerValue](1000, 1, time.Hour, 10,
				sturdyc.WithNoContinuousEvictions(),
				sturdyc.WithMaxBytes(1<<30),
			)

			key := strings.Repeat("x", tt.keyLen)
			value := testSizerValue{data: make([]byte, tt.valueSize)}
			c.Set(key, value)

			actualSize := c.SizeBytes()

			// Verify size is positive
			assertThat(t, actualSize > 0, "expected positive size, got %d", actualSize)

			// Verify size is within expected bounds:
			// Should be at least valueSize + keyLen + minimal overhead
			minSize := tt.valueSize + uint32(tt.keyLen)
			assertThat(t, actualSize >= uint64(minSize), "size %d is less than minimum expected %d", actualSize, minSize)

			// Should be at most valueSize + keyLen + substantial overhead
			maxSize := tt.valueSize + uint32(tt.keyLen) + 200
			assertThat(t, actualSize <= uint64(maxSize), "size %d exceeds maximum expected %d", actualSize, maxSize)
		})
	}
}

type testSizerValue struct {
	data []byte
}

func (v testSizerValue) Size() uint32 {
	return uint32(len(v.data))
}

// assertThat is a helper function that reports an error if the condition is false.
func assertThat(t *testing.T, cond bool, msg string, args ...any) {
	t.Helper()
	if !cond {
		t.Errorf(msg, args...)
	}
}

// assertPanic tests that a function panics as expected.
func assertPanic(t *testing.T, fn func(), msg string) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf(msg)
		}
	}()
	fn()
}
