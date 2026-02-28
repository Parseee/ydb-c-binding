from . cimport ydb_c
from cpython.mem cimport PyMem_Malloc, PyMem_Free

cdef class Result:
    cdef ydb_c.YdbResult* _result
    cdef bint _closed

    def __init__(self):
        raise TypeError('Cannot create instance from Python')

    @staticmethod
    cdef Result create(ydb_c.YdbResult* result):
        res = <Result>Result.__new__(Result) # create instance without call __init__
        res._closed = False
        res._result = result
        # print("result received {0:x}", <unsigned long> res._result)
        return res

    def close(self):
        if self._closed:
            return

        self._closed = True
        # print("closing result {0:x}", <unsigned long> self._result)
        with nogil:
            ydb_c.ydb_result_free(self._result)

    def __del__(self):
        self.close()

    cpdef wait(self):
        # print("wait result", <unsigned long> self._result)
        with nogil:
            ydb_c.ydb_result_wait(self._result)

    cdef _ensure_no_errors(self):
        if ydb_c.ydb_result_has_errors(self._result) != 0:
            raise Exception("Ydb result has errors.")

    def to_results(self):
        cdef size_t bufSize = 1024
        cdef char* mem = NULL
        try:
            mem = <char*>PyMem_Malloc(bufSize)
            results = []
            while ydb_c.ydb_result_next_readset(self._result) == 0:
                result = []
                results.append(result)
                while ydb_c.ydb_result_next_row(self._result) == 0:
                    row = {}
                    result.append(row)
                    ydb_c.ydb_result_read_first_field_text(self._result, mem, <int>bufSize)
                    row["first"] = mem.decode()
            return results
        finally:
            if mem != NULL:
                PyMem_Free(mem)

    cpdef next_row(self):
        ydb_c.ydb_result_next_row(self._result)
        self._ensure_no_errors()


cdef class Connection:
    cdef ydb_c.YdbConnection* _connection
    cdef bint _closed

    def __init__(self):
        raise TypeError('Cannot create instance from Python')

    @staticmethod
    cdef Connection create(ydb_c.YdbConnection* connection_handler):
        res = <Connection>Connection.__new__(Connection) # create instance without call __init__
        res._closed = False
        res._connection = connection_handler
        print("inited {0:x}", <unsigned long> res._connection)
        return res

    def query(self, query: str)->Result:
        # print("rekby-1")
        query_bytes_py = query.encode()
        cdef char* query_bytes = query_bytes_py
        # print("query on connection ", <unsigned long> self._connection)
        with nogil:
            res_c = ydb_c.ydb_query(self._connection, query_bytes)
        # print("rekby-2", res_c != NULL)
        res = Result.create(res_c)
        # print("rekby-2.1", res_c != NULL)
        res.wait()
        # print("rekby-2.2", res_c != NULL)

        res._ensure_no_errors()

        return res


    def close(self):
        if self._closed:
            return

        self._closed = True
        print("closing {0:x}", <unsigned long> self._connection)
        ydb_c.ydb_connect_free(self._connection)

    def __del__(self):
        self.close()


def open(str connection_string) -> Connection:
    connection_string_bytes = connection_string.encode()
    cdef char *connection_string_bytes_pointer = connection_string_bytes
    with nogil:
        connection_handler = ydb_c.ydb_connect(connection_string_bytes_pointer)
        if ydb_c.ydb_connect_wait(connection_handler) != 0:
            raise Exception("Ydb connection error")

    return Connection.create(connection_handler)
