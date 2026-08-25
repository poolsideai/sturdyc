package sturdyc

// Sizer is an interface that types can implement to report their memory size in bytes.
// This is used by the cache when MaxBytes is configured for memory-based eviction.
// The implementation should return the combined size of the key and value in bytes.
//
// Example:
//
//	type MyValue struct {
//		data []byte
//	}
//
//	func (v *MyValue) Size() int {
//		return len(v.data)
//	}
type Sizer interface {
	Size() uint32
}
