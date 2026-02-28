package main

import "C"

import (
	"unsafe"

	"github.com/rekby/safemutex"
)

func ydbConnectionToC(conn mconnectionState) *C.struct_YdbConnection {
	p := unsafe.Pointer(NewPointer(conn))

	return (*C.struct_YdbConnection)(p)
}

func ydbConnectionToGo(connection *C.struct_YdbConnection) *CPointer[safemutex.RWMutexWithPointers[connectionState]] {
	cpointer := (*CPointer[safemutex.RWMutexWithPointers[connectionState]])(unsafe.Pointer(connection))
	cpointer.EnsureValid()

	return cpointer
}

func ydbResultToC(res mpqueryState) *C.struct_YdbResult {
	p := unsafe.Pointer(NewPointer(res))
	return (*C.struct_YdbResult)(p)
}

func ydbResultToGo(res *C.struct_YdbResult) *CPointer[mqueryState] {
	cpointer := (*CPointer[mqueryState])(unsafe.Pointer(res))
	cpointer.EnsureValid()

	return cpointer
}
