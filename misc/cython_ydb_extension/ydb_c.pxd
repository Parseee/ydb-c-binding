cdef extern from "ydb_interface.h":
    struct YdbConnection:
        pass

    struct YdbResult:
        pass

    YdbConnection* ydb_connect(char *connectionString) nogil
    int ydb_connect_wait(YdbConnection* connection) nogil
    void ydb_connect_free(YdbConnection* connection) nogil

    YdbResult* ydb_query(YdbConnection* connection, char* query) nogil
    void ydb_result_wait(YdbResult* res) nogil
    int ydb_result_has_errors(YdbResult* res) nogil
    int ydb_result_next_readset(YdbResult* res) nogil
    int ydb_result_next_row(YdbResult* res) nogil
    int ydb_result_read_first_field_text(YdbResult* res, void* dstBuffer, int dstBufferLen) nogil
    void ydb_result_free(YdbResult* res) nogil
