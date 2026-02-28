package main

import "C"

import (
	"unsafe"

	"github.com/rekby/safemutex"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
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

func ydbTopicReaderToC(reader *topicreader.Reader) C.ulong {
	cpointer := NewPointer(reader)
	return C.ulong(uintptr(unsafe.Pointer(cpointer)))
}

func ydbTopicReaderToGo(reader C.ulong) *CPointer[topicreader.Reader] {
	cpointer := (*CPointer[topicreader.Reader])(unsafe.Pointer(uintptr(reader)))
	cpointer.EnsureValid()
	return cpointer
}
