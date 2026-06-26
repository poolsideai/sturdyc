package sturdyc_test

import (
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viccon/sturdyc"
)

// This benchmark tests the two eviction policies (TTL/SIEVE) under different kinds of workloads.

// workload generates cache keys based on an access pattern.
type workload struct {
	name string
	// newGen returns a fresh key generator for the i-th operation.
	// Each call returns an independent generator so that different
	// benchmark sub-runs use identical access sequences.
	newGen func() func(i int) string
}

// zipfianWorkload creates a power-law access pattern where a small set of keys
// are accessed very frequently and most keys are rarely accessed.
func zipfianWorkload(keySpace uint64, s float64) workload {
	return workload{
		name: "Zipfian",
		newGen: func() func(int) string {
			src := rand.New(rand.NewPCG(42, 0))
			zipf := rand.NewZipf(src, s, 1.0, keySpace-1)
			return func(_ int) string {
				return strconv.FormatUint(zipf.Uint64(), 10)
			}
		},
	}
}

// temporalShiftWorkload creates a pattern where the hot key set shifts
// periodically. Tests how quickly eviction adapts to changing access patterns.
func temporalShiftWorkload(hotSetSize, shiftEvery int) workload {
	return workload{
		name: "TemporalShift",
		newGen: func() func(int) string {
			src := rand.New(rand.NewPCG(42, 0))
			return func(i int) string {
				phase := i / shiftEvery
				baseKey := phase * hotSetSize
				return strconv.Itoa(baseKey + src.IntN(hotSetSize))
			}
		},
	}
}

// scanWorkload creates a three-phase pattern:
//  1. Warm up a working set (keys 0..workingSetSize-1)
//  2. Sequential scan of one-time keys (simulating a batch job)
//  3. Return to the original working set
//
// Tests whether eviction resists pollution from the scan.
func scanWorkload(workingSetSize, scanSize int) workload {
	warmupOps := workingSetSize * 5
	return workload{
		name: "ScanResistant",
		newGen: func() func(int) string {
			src := rand.New(rand.NewPCG(42, 0))
			return func(i int) string {
				switch {
				case i < warmupOps:
					return strconv.Itoa(src.IntN(workingSetSize))
				case i < warmupOps+scanSize:
					scanKey := workingSetSize + (i - warmupOps)
					return strconv.Itoa(scanKey)
				default:
					return strconv.Itoa(src.IntN(workingSetSize))
				}
			}
		},
	}
}

type evictionStrategy struct {
	name string
	opts []sturdyc.Option
}

// evictionLimit defines which limit triggers eviction.
type evictionLimit struct {
	name     string
	capacity int
	maxBytes uint64
	newValue func() *valueType
}

// uniformValueSize returns a factory that always produces values of the given size.
func uniformValueSize(size int) func() *valueType {
	return func() *valueType { return NewValue(size) }
}

// randomValueSize returns a factory that produces values with sizes
// uniformly distributed between minSize and maxSize.
func randomValueSize(minSize, maxSize int) func() *valueType {
	src := rand.New(rand.NewPCG(99, 0))
	return func() *valueType {
		size := minSize + src.IntN(maxSize-minSize+1)
		return NewValue(size)
	}
}

func runEvictionBenchmark(b *testing.B, limit evictionLimit) {
	const (
		numShards          = 10
		ttl                = time.Hour
		evictionPercentage = 5
	)

	workloads := []workload{
		zipfianWorkload(10_000, 1.01),
		temporalShiftWorkload(300, 5_000),
		scanWorkload(500, 5_000),
	}

	strategies := []evictionStrategy{
		{"TTL", nil},
		{"SIEVE", []sturdyc.Option{sturdyc.WithSIEVE()}},
	}

	for _, strategy := range strategies {
		for _, wl := range workloads {
			b.Run(strategy.name+"/"+wl.name, func(b *testing.B) {
				metricsRecorder := &recorder{}
				opts := []sturdyc.Option{
					sturdyc.WithNoContinuousEvictions(),
					sturdyc.WithMetrics(metricsRecorder),
				}
				if limit.maxBytes > 0 {
					opts = append(opts, sturdyc.WithMaxBytes(limit.maxBytes))
				}
				opts = append(opts, strategy.opts...)
				c := sturdyc.New[*valueType](limit.capacity, numShards, ttl, evictionPercentage, opts...)

				gen := wl.newGen()
				var hits, total int
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					key := gen(i)
					if _, ok := c.Get(key); ok {
						hits++
					} else {
						c.Set(key, limit.newValue())
					}
					total++
				}
				b.StopTimer()
				if total > 0 {
					b.ReportMetric(float64(hits), "hits")
					b.ReportMetric(float64(total-hits), "misses")
					b.ReportMetric(float64(hits)/float64(total), "hit-ratio")
					b.ReportMetric(float64(metricsRecorder.entriesEvicted.Load()), "entries-evicted")
				}
			})
		}
	}
}

// BenchmarkEvictionByCapacity tests eviction triggered by entry count limit.
// MaxBytes is not configured, so only capacity triggers eviction.
func BenchmarkEvictionByCapacity(b *testing.B) {
	runEvictionBenchmark(b, evictionLimit{
		name:     "Capacity",
		capacity: 1_000,
		maxBytes: 0,
		newValue: uniformValueSize(4096),
	})
}

// BenchmarkEvictionByBytes tests eviction triggered by memory size limit.
// Capacity is set very high so only maxBytes triggers eviction.
func BenchmarkEvictionByBytes(b *testing.B) {
	valueSizes := []struct {
		name     string
		newValue func() *valueType
	}{
		{"Uniform4KB", uniformValueSize(4096)},
		{"Random1KB-16KB", randomValueSize(1024, 16384)},
	}

	for _, vs := range valueSizes {
		b.Run(vs.name, func(b *testing.B) {
			// Each value is ~4KB on average + entry overhead.
			// 1000 * 1024 bytes allows roughly ~240 entries before eviction.
			runEvictionBenchmark(b, evictionLimit{
				name:     "Bytes",
				capacity: 1_000_000,
				maxBytes: 1_000 * 1024,
				newValue: vs.newValue,
			})
		})
	}
}

type valueType struct {
	bytes []byte
}

func (v *valueType) Size() uint32 {
	return uint32(len(v.bytes))
}

func NewValue(size int) *valueType {
	return &valueType{bytes: make([]byte, size)}
}

type recorder struct {
	entriesEvicted atomic.Int32
}

func (r *recorder) CacheHit() {
}

func (r *recorder) CacheMiss() {
}

func (r *recorder) AsynchronousRefresh() {
}

func (r *recorder) SynchronousRefresh() {
}

func (r *recorder) MissingRecord() {
}

func (r *recorder) ForcedEviction() {
}

func (r *recorder) EntriesEvicted(i int) {
	r.entriesEvicted.Add(int32(i))
}

func (r *recorder) ShardIndex(_ int) {
}

func (r *recorder) CacheBatchRefreshSize(_ int) {
}

func (r *recorder) ObserveCacheSize(_ func() int) {
}

func (r *recorder) ObserveCacheSizeBytes(_ func() uint64) {
}

var _ sturdyc.MetricsRecorder = (*recorder)(nil)
