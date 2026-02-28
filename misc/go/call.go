package main

type CallState interface {
	IsDone() bool
	WaitDone()
}
