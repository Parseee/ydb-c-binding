package main

import (
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/rekby/safemutex"
)

var globalPointers = safemutex.NewWithPointers[pointerMap](pointerMap{pointers: map[unsafe.Pointer]PointerInterface{}})

type pointerMap struct {
	pointers map[unsafe.Pointer]PointerInterface
}

type PointerInterface interface {
	EnsureValid()
	Free()
}

type CPointer[T any] struct {
	data     *T
	freed    atomic.Bool
	pinner   *runtime.Pinner
	pointers *safemutex.MutexWithPointers[pointerMap]
}

func NewPointer[T any](data *T) *CPointer[T] {
	return newPointer(&globalPointers, data)
}

func newPointer[T any](m *safemutex.MutexWithPointers[pointerMap], data *T) *CPointer[T] {
	res := &CPointer[T]{
		data:     data,
		pinner:   &runtime.Pinner{},
		pointers: m,
	}
	res.pinner.Pin(res)
	res.pinner.Pin(res.data)
	res.pinner.Pin(res.pinner)
	p := unsafe.Pointer(res)
	globalPointers.Lock(func(m pointerMap) pointerMap {
		m.pointers[p] = res
		return m
	})
	return res
}

var _ PointerInterface = &CPointer[int]{}

func (p *CPointer[T]) EnsureValid() {
	if p.freed.Load() {
		panic("the object was free already")
	}
}

func (p *CPointer[T]) Free() {
	wasFree := p.freed.Swap(true)
	if wasFree {
		panic("double free the value")
	}

	pointer := unsafe.Pointer(p)
	p.pointers.Lock(func(m pointerMap) pointerMap {
		delete(m.pointers, pointer)
		return m
	})
	p.pinner.Unpin()
}

func (p *CPointer[T]) Data() *T {
	p.EnsureValid()
	return p.data
}
