package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"runtime"
	"time"
	"unsafe"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// TODO: detect and set lib name by auto to LDFLAGS
// install pkg-config

/*
#cgo pkg-config: python-3.12
#cgo LDFLAGS: -lpython3.12

#include "c_bind_python.h"
*/
import "C"

///
/// Python module
///

//export python_connect
func python_connect(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var cSize C.ulong
	var cPointer *C.char

	var res = C._py_read_one_string_arg(args, &cPointer, &cSize)
	fmt.Println("_py_read_one_string_arg res:", res)

	connString := C.GoStringN(cPointer, C.int(cSize))

	connStatePointer := ydb_connect(connString)

	log.Println("created conn state pointer: ", connStatePointer)
	return C.PyLong_FromUnsignedLong(connStatePointer)
}

//export ydb_python_start_reader
func ydb_python_start_reader(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var connPointerC C.ulong
	var topicNameSize C.ulong
	var topicNamePointer *C.char
	var consumerNameSize C.ulong
	var consumerNamePointer *C.char

	var res = C._py_read_ulong_string_string(args, &connPointerC,
		&topicNamePointer, &topicNameSize,
		&consumerNamePointer, &consumerNameSize,
	)
	log.Println("read result of ydb_python_start_reader: ", res)
	log.Println("receiver conn state pointer: ", connPointerC)

	topicName := C.GoStringN(topicNamePointer, C.int(topicNameSize))
	consumerName := C.GoStringN(consumerNamePointer, C.int(consumerNameSize))

	connState := ydbConnectionToGo((*C.struct_YdbConnection)(unsafe.Pointer(uintptr(connPointerC))))
	var topicReader *topicreader.Reader
	connState.data.RLock(func(synced connectionState) {
		log.Println("driver address:", synced.driver)
		reader, err := synced.driver.Topic().StartReader(
			consumerName,
			topicoptions.ReadTopic(topicName),
			topicoptions.WithReaderBufferSizeBytes(1024*1024),
		)
		if err != nil {
			log.Fatalf("failed to start reader: %+v", err)
		}
		topicReader = reader

		// WARMUP, for test purposes only
		ctxWarmup, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _ = reader.ReadMessage(ctxWarmup)
		cancel()
	})

	return C.PyLong_FromUnsignedLong(ydbTopicReaderToC(topicReader))
}

//export ydb_python_read_batch
func ydb_python_read_batch(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var preaderC C.ulong
	_ = C._py_read_ulong(args, &preaderC)

	// res := C._py_read_ulong(args, &preaderC)
	// log.Printf("get reader pointer result: %v, pointer: %v", res, preaderC)

	preader := ydbTopicReaderToGo(preaderC).data

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	batch, err := preader.ReadMessagesBatch(ctx)

	if ctx.Err() != nil {
		return C._py_none()
	}
	if err != nil {
		log.Fatalf("failed to read messages: %+v", err)
	}

	// log.Println("Topic read messages count: ", len(batch.Messages))

	return convertBatchToPython(batch)
}

func convertBatchToPython(batch *topicreader.Batch) *C.PyObject {
	cBatch, pinner := convertBatchToC(batch)
	defer freeMessagesBatch(cBatch, pinner)

	return C.convertCBatchToPython(cBatch)
}

func convertBatchToC(batch *topicreader.Batch) (C.MessagesBatch, *runtime.Pinner) {
	pinner := &runtime.Pinner{}
	var cBatch C.MessagesBatch
	cBatch.messages_count = C.ulong(len(batch.Messages))
	cBatch.messages = (*C.Message)(C.malloc(C.sizeof_Message * cBatch.messages_count))

	buf := &bytes.Buffer{}
	for i := range batch.Messages {
		gMessage := batch.Messages[i]

		buf.Reset()
		io.Copy(buf, gMessage)

		startMessagesPointer := unsafe.Pointer(cBatch.messages)
		cMessageUnsafePointer := unsafe.Add(startMessagesPointer, C.sizeof_Message*C.ulong(i))
		cMessage := (*C.Message)(cMessageUnsafePointer)

		cMessage.seq_no = C.long(gMessage.SeqNo)
		cMessage.created_at_timestamp_ms = C.long(gMessage.CreatedAt.UnixMilli())
		cMessage.message_group_id, cMessage.message_group_id_len = copyStringToC(gMessage.MessageGroupID, pinner)
		cMessage.offset = C.long(gMessage.Offset)
		cMessage.written_at_timestamp_ms = C.long(gMessage.WrittenAt.UnixMilli())
		cMessage.producer_id, cMessage.producer_id_len = copyStringToC(gMessage.ProducerID, pinner)
		cMessage.data, cMessage.data_len = copyBytesToC(buf.Bytes(), pinner)
	}

	return cBatch, pinner
}

func freeMessagesBatch(cBatch C.MessagesBatch, pinner *runtime.Pinner) {
	pinner.Unpin()
	// log.Println("Unpin for messages batch called")

	for i := C.ulong(0); i < cBatch.messages_count; i++ {
		startMessagesPointer := unsafe.Pointer(cBatch.messages)
		cMessageUnsafePointer := unsafe.Add(startMessagesPointer, C.sizeof_Message*C.ulong(i))
		cMessage := (*C.Message)(cMessageUnsafePointer)

		if cMessage.producer_id != nil {
			C.free(unsafe.Pointer(cMessage.producer_id))
		}
		if cMessage.message_group_id != nil {
			C.free(unsafe.Pointer(cMessage.message_group_id))
		}
		if cMessage.data != nil {
			// log.Printf("Free: %v", unsafe.Pointer(cMessage.data))
			C.free(unsafe.Pointer(cMessage.data))
		}
	}

	C.free(unsafe.Pointer(cBatch.messages))
	// log.Printf("Free: %v", unsafe.Pointer(cBatch.messages))
}

func toPyString(s string) *C.PyObject {
	sLen := len(s)
	if sLen == 0 {
		return C.PyUnicode_FromStringAndSize(nil, 0)
	}

	pinner := runtime.Pinner{}
	// cPointer := uintptr(unsafe.Pointer(stringBytes))
	cPointer := C._GoStringPtr(s)
	pinner.Pin(cPointer)
	res := C.PyUnicode_FromStringAndSize(cPointer, C.long(sLen))
	pinner.Unpin()

	return res
}

func copyBytesToC(data []byte, pinner *runtime.Pinner) (*C.char, C.long) {
	if len(data) == 0 {
		return nil, 0
	}

	cData := (*C.char)(C.CBytes(data))
	// pData := unsafe.SliceData(data)
	// pinner.Pin(pData)
	// cData := (*C.char)(unsafe.Pointer(pData))

	return cData, C.long(len(data))
}

//go:nosplit
func copyStringToC(s string, pinner *runtime.Pinner) (*C.char, C.long) {
	sDataPointer := unsafe.StringData(s)
	// pinner.Pin(sDataPointer)
	// log.Print("pin ", sDataPointer)
	// res := (*C.char)(unsafe.Pointer(sDataPointer))
	res := C.CString(s)
	runtime.KeepAlive(s)
	runtime.KeepAlive(sDataPointer)
	return res, C.long(len(s))
}

func toInt(v int64) *C.PyObject {
	return C.PyLong_FromLong(C.long(v))
}
