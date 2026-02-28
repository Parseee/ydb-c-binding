#define PY_SSIZE_T_CLEAN
#include <Python.h>

PyObject* ydb_python_read_result(PyObject* self, PyObject* args);
PyObject* python_connect(PyObject* self, PyObject* args);
PyObject* ydb_python_start_reader(PyObject* self, PyObject* args);
PyObject* ydb_python_read_batch(PyObject* self, PyObject* args);

int _py_read_ulong_string_string(PyObject *args, unsigned long *lval, char **str1, size_t *str1Len, char **str2, size_t *str2Len);
int _py_read_one_string_arg(PyObject *args, char **content, size_t *bytes_len);
int _py_read_ulong(PyObject *args, unsigned long *val);

PyObject *_py_none();

#ifndef __C_BIND_PYTHON_H
#define __C_BIND_PYTHON_H

typedef struct Message {
    long seq_no;
    long created_at_timestamp_ms;
    const char *message_group_id;
    long message_group_id_len;
    long offset;
    long written_at_timestamp_ms;
    const char *producer_id;
    long producer_id_len;
    const char *data;
    long data_len;
} Message;

typedef struct MessagesBatch {
    unsigned long messages_count;
    Message *messages;
} MessagesBatch;


/*
type PublicMessage struct {
	empty.DoNotCopy

	SeqNo                int64
	CreatedAt            time.Time
	MessageGroupID       string
	WriteSessionMetadata map[string]string
	Offset               int64
	WrittenAt            time.Time
	ProducerID           string
	Metadata             map[string][]byte // Metadata, nil if no metadata

	commitRange        commitRange
	data               oneTimeReader
	rawDataLen         int
	bufferBytesAccount int
	UncompressedSize   int // as sent by sender, server/sdk doesn't check the field. It may be empty or wrong.
	dataConsumed       bool
}
*/

#endif // __C_BIND_PYTHON_H

PyObject *convertCBatchToPython(MessagesBatch cBatch);
