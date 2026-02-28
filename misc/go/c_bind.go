package main

/*

typedef struct YdbConnection {} YdbConnection;
typedef struct YdbResult {} YdbResult;

typedef struct YdbUserCustomData {} YdbUserCustomData;
typedef void (*YdbResultCallback)(YdbResult *result, YdbUserCustomData *data);


#include <string.h>

void c_helper_call_ydb_result_callback(YdbResultCallback cb, YdbResult *result, YdbUserCustomData *data);
*/
import "C"
import (
	"context"
	"fmt"
	"log"
	"unsafe"
)

//export ydb_connect
func ydb_connect(connectionString *C.char) *C.struct_YdbConnection {
	connString := C.GoString(connectionString)
	connectionState := startConnect(connString)

	return ydbConnectionToC(connectionState)
}

//export ydb_connect_has_result
func ydb_connect_has_result(connection *C.struct_YdbConnection) (hasResult C.int) {
	cpointer := ydbConnectionToGo(connection)

	connMutex := cpointer.Data()

	connMutex.RLock(func(synced connectionState) {
		if synced.IsDone() {
			hasResult = 1
		} else {
			hasResult = 0
		}
	})
	return hasResult
}

//export ydb_connect_wait
func ydb_connect_wait(connection *C.struct_YdbConnection) (hasErrors C.int) {
	cpointer := ydbConnectionToGo(connection)

	connMutex := cpointer.Data()

	var done chan struct{}
	connMutex.RLock(func(synced connectionState) {
		done = synced.done
	})

	<-done
	connMutex.RLock(func(synced connectionState) {
		if synced.err == nil {
			hasErrors = 0
		} else {
			fmt.Println("err: ", synced.err)
			hasErrors = 1
		}
	})

	return hasErrors
}

//export ydb_connect_free
func ydb_connect_free(connection *C.struct_YdbConnection) {
	cpointer := ydbConnectionToGo(connection)

	freeConnect(cpointer.data)
	cpointer.Free()
}

//export ydb_query
func ydb_query(connection *C.struct_YdbConnection, query *C.char) *C.struct_YdbResult {
	cpointer := ydbConnectionToGo(connection)

	queryS := C.GoString(query)
	queryState := executeQuery(cpointer.Data(), queryS)

	return ydbResultToC(queryState)
}

//export ydb_result_free
func ydb_result_free(res *C.struct_YdbResult) {
	cpointer := ydbResultToGo(res)
	ydbResultFree(cpointer.Data())

	cpointer.Free()
}

//export ydb_result_has_result
func ydb_result_has_result(res *C.struct_YdbResult) (hasResult C.int) {
	cpointer := ydbResultToGo(res)
	state := cpointer.Data()
	state.RLock(func(synced queryState) {
		if synced.IsDone() {
			hasResult = 1
		} else {
			hasResult = 0
		}
	})
	return hasResult
}

//export ydb_result_wait
func ydb_result_wait(res *C.struct_YdbResult) {
	cpointer := ydbResultToGo(res)
	state := cpointer.Data()
	var done chan struct{}
	state.RLock(func(synced queryState) {
		done = synced.done
	})
	<-done
}

//export ydb_result_callback
func ydb_result_callback(res *C.struct_YdbResult, callback C.YdbResultCallback, userData *C.YdbUserCustomData) (hasError C.int) {
	cpointer := ydbResultToGo(res)

	var hasResult chan struct{}
	cpointer.Data().RLock(func(synced queryState) {
		hasResult = synced.done
	})

	go func() {
		<-hasResult
		C.c_helper_call_ydb_result_callback(callback, res, userData)
	}()
	return 0
}

//export ydb_result_has_errors
func ydb_result_has_errors(res *C.struct_YdbResult) (hasError C.int) {
	cpointer := ydbResultToGo(res)
	cpointer.Data().RLock(func(ydbResult queryState) {
		if ydbResult.err == nil {
			hasError = 0
		} else {
			hasError = 1
			log.Printf("result error: %+v", ydbResult.err)
		}
	})
	return hasError
}

//export ydb_result_next_readset
func ydb_result_next_readset(res *C.struct_YdbResult) (hasError C.int) {
	cpointer := ydbResultToGo(res)
	cpointer.Data().Lock(func(ydbResult queryState) queryState {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		if ydbResult.res.NextResultSet(ctx) {
			hasError = 0
		} else {
			hasError = 1
		}
		cancel()
		return ydbResult
	})
	return hasError
}

//export ydb_result_next_row
func ydb_result_next_row(res *C.struct_YdbResult) (hasError C.int) {
	cpointer := ydbResultToGo(res)
	cpointer.Data().Lock(func(ydbResult queryState) queryState {
		if ydbResult.res.NextRow() {
			hasError = 0
		} else {
			hasError = 1
		}
		return ydbResult
	})
	return hasError
}

//export ydb_result_read_first_field_text
func ydb_result_read_first_field_text(res *C.struct_YdbResult, dstBuffer unsafe.Pointer, dstBufferLen C.int) (hasError C.int) {
	cpointer := ydbResultToGo(res)
	var fieldValue string
	cpointer.Data().RLock(func(ydbResult queryState) {
		err := ydbResult.res.ScanWithDefaults(&fieldValue)
		if err == nil {
			hasError = 0
		} else {
			log.Printf("scan field error: %+v", err)
			hasError = 1
		}
	})
	if hasError != 0 {
		return
	}

	if int(dstBufferLen-1) < len(fieldValue) {
		log.Printf("buffer is small, buffer size: %v bytes, need: %v bytes", int(dstBufferLen), len(fieldValue)+1)
		return 1
	}

	fieldData := unsafe.StringData(fieldValue)
	C.memcpy(dstBuffer, unsafe.Pointer(fieldData), C.size_t(len(fieldValue)))
	endOfLine := unsafe.Add(dstBuffer, len(fieldValue))
	*(*byte)(endOfLine) = 0

	return 0
}

//export ydb_check_linked_library
func ydb_check_linked_library() C.int {
	return 1
}
