package plru

import _ "unsafe"

//go:linkname stringHash runtime.stringHash
func stringHash(s string, seed uintptr) uintptr

//go:linkname int64Hash runtime.int64Hash
func int64Hash(v uint64, seed uintptr) uintptr

var Hash = struct {
	Str    func(v string) uint64
	Int    func(v int) uint64
	Int64  func(v int64) uint64
	Uint32 func(v uint32) uint64
	Uint64 func(v uint64) uint64
}{
	func(v string) uint64 { return uint64(stringHash(v, 0)) },
	func(v int) uint64 { return uint64(int64Hash(uint64(v), 0)) },
	func(v int64) uint64 { return uint64(int64Hash(uint64(v), 0)) },
	func(v uint32) uint64 { return uint64(int64Hash(uint64(v), 0)) },
	func(v uint64) uint64 { return uint64(int64Hash(uint64(v), 0)) },
}
