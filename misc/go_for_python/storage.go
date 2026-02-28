package main

import (
	"github.com/rekby/safemutex"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	globalConnections = &safemutex.RWMutexWithPointers[*connectionStorage]{}
)

func init() {
	globalConnections.Lock(func(_ *connectionStorage) *connectionStorage {
		return &connectionStorage{
			connections: map[int]*safemutex.RWMutexWithPointers[connectionState]{},
		}
	})
}

type connectionStorage struct {
	lastVal     int
	connections map[int]*safemutex.RWMutexWithPointers[connectionState]
}

func (cs *connectionStorage) NextID() int {
	cs.lastVal++
	return cs.lastVal
}

type connectionState struct {
	done   chan struct{}
	driver *ydb.Driver
	err    error
}
type mconnectionState = *safemutex.RWMutexWithPointers[connectionState]

var _ CallState = &connectionState{}

func (cs *connectionState) IsDone() bool {
	select {
	case <-cs.done:
		return true
	default:
		return false
	}
}

func (cs *connectionState) WaitDone() {
	<-cs.done
}
