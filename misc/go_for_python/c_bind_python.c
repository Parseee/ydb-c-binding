#include "c_bind_python.h"

// functions, exported as python module function
static struct PyMethodDef methods[] = {
    {"start_reader", (PyCFunction)ydb_python_start_reader, METH_VARARGS},
    {"connect", (PyCFunction)python_connect, METH_VARARGS},
    {"read_batch", (PyCFunction)ydb_python_read_batch, METH_VARARGS},
    {NULL, NULL}
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "go_for_python", // module name, same as for PyInit_...
    NULL,
    -1,
    methods,
};

PyMODINIT_FUNC PyInit_go_for_python(void) {
    return PyModule_Create(&module);
};

int _py_read_one_string_arg(PyObject *args, char **content, size_t *bytes_len){
    return PyArg_ParseTuple(args, "s#", content, bytes_len);
};

int _py_read_ulong_string_string(PyObject *args, unsigned long *lval, char **str1, size_t *str1Len, char **str2, size_t *str2Len){
	return PyArg_ParseTuple(args, "ks#s#", lval, str1, str1Len, str2, str2Len);
};

int _py_read_ulong(PyObject *args, unsigned long *val){
	return PyArg_ParseTuple(args, "k", val);
};

PyObject *_py_none(){
    Py_RETURN_NONE;
};

PyObject* convertCMessageToPython(Message* cMess){
    PyObject* pyMess = PyDict_New();

    PyDict_SetItemString(pyMess, "seq_no", PyLong_FromLong(cMess->seq_no));
    PyDict_SetItemString(pyMess, "created_at_timestamp_ms", PyLong_FromLong(cMess->created_at_timestamp_ms));
    PyDict_SetItemString(pyMess, "message_group_id", PyUnicode_FromStringAndSize(cMess->message_group_id, cMess->message_group_id_len));
    PyDict_SetItemString(pyMess, "offset", PyLong_FromLong(cMess->offset));
    PyDict_SetItemString(pyMess, "written_at_timestamp_ms", PyLong_FromLong(cMess->written_at_timestamp_ms));
    PyDict_SetItemString(pyMess, "producer_id", PyUnicode_FromStringAndSize(cMess->producer_id, cMess->producer_id_len));
    PyDict_SetItemString(pyMess, "data", PyBytes_FromStringAndSize(cMess->data, cMess->data_len));

    return pyMess;
}

PyObject *convertCBatchToPython(MessagesBatch cBatch) {
    PyObject *pyBatch = PyDict_New();

    // PyObject *pyMessages = PyList_New(1);
    PyObject *pyMessages = PyList_New(cBatch.messages_count);
    PyDict_SetItemString(pyBatch, "messages", pyMessages);

    // create message objects
    for (unsigned long i = 0; i < cBatch.messages_count; i++){
        PyObject *pyMess = convertCMessageToPython(&cBatch.messages[i]);
        PyList_SetItem(pyMessages, i, pyMess);
    };

	return pyBatch;
}
