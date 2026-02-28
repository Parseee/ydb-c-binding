package main

import (
	"context"
	"fmt"

	"github.com/rekby/safemutex"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

type queryState struct {
	done chan struct{}
	err  error
	res  result.Result
}
type mqueryState = safemutex.RWMutexWithPointers[queryState]
type mpqueryState = *mqueryState

// IsDone implements CallState.
func (s *queryState) IsDone() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// WaitDone implements CallState.
func (s *queryState) WaitDone() {
	<-s.done
}

var _ CallState = &queryState{}

func executeQuery(s mconnectionState, query string) mpqueryState {
	statePointer := &mqueryState{}
	statePointer.Lock(func(state queryState) queryState {
		state.done = make(chan struct{})
		return state
	})

	var driver *ydb.Driver
	var err error
	s.RLock(func(synced connectionState) {
		driver = synced.driver
		err = synced.err
	})

	if err != nil {
		statePointer.Lock(func(state queryState) queryState {
			state.err = fmt.Errorf("failed connect to the server: %w", err)
			close(state.done)
			return state
		})

		return statePointer
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		var tableRes result.Result
		err := driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			_, res, err := s.Execute(ctx, table.DefaultTxControl(), query, nil)
			if err != nil {
				return err
			}
			tableRes = res
			return nil
		})
		cancel()
		statePointer.Lock(func(state queryState) queryState {
			state.err = err
			state.res = tableRes
			close(state.done)
			return state
		})
	}()

	return statePointer
}

func ydbResultFree(state mpqueryState) {
	state.Lock(func(synced queryState) queryState {
		if synced.res != nil {
			synced.res.Close()
		}
		return synced
	})

}
