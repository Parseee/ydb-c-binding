package main

import (
	"context"
	"time"

	"github.com/rekby/safemutex"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const operationTimeout = time.Second * 5

func startConnect(connectionString string) mconnectionState {
	connStateMutex := &safemutex.RWMutexWithPointers[connectionState]{}
	connStateMutex.Lock(func(synced connectionState) connectionState {
		synced.done = make(chan struct{})
		return synced
	})

	go func() {
		ctx, ctxCancel := context.WithTimeout(context.Background(), operationTimeout)
		driver, err := ydb.Open(ctx, connectionString)
		ctxCancel()
		connStateMutex.Lock(func(state connectionState) connectionState {
			state.driver = driver
			state.err = err
			close(state.done)
			return state
		})
	}()

	return connStateMutex
}

func freeConnect(s mconnectionState) {
	s.Lock(func(s connectionState) connectionState {
		if s.driver != nil {
			ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
			s.driver.Close(ctx)
			cancel()
		}
		return s
	})
}
