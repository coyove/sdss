package plru

import (
	"reflect"
	"strconv"
	"unsafe"
)

func (m *Map[K, V]) UnsafeBytes() []byte {
	slice := *(*reflect.SliceHeader)(unsafe.Pointer(&m.items))
	slice.Len *= int(unsafe.Sizeof(hashItem[K, V]{}))
	slice.Cap = slice.Len
	return *(*[]byte)(unsafe.Pointer(&slice))
}

func (m *Map[K, V]) UnsafeSetBytes(buf []byte) {
	el := int(unsafe.Sizeof(hashItem[K, V]{}))
	if len(buf)/el*el != el {
		panic("UnsafeSetRef: invalid bytes size " + strconv.Itoa(len(buf)))
	}

	slice := *(*reflect.SliceHeader)(unsafe.Pointer(&buf))
	slice.Len = slice.Len / el
	slice.Cap = slice.Len
	m.items = *(*[]hashItem[K, V])(unsafe.Pointer(&buf))
}
