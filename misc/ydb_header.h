typedef struct YdbUserCustomData {} YdbUserCustomData;

typedef struct YdbConnection {} YdbConnection;
typedef struct YdbResult {} YdbResult;

typedef void (YdbResultCallback)(YdbResult *result, YdbUserCustomData *data);

extern struct YdbConnection* ydb_connect(char* connectionString);
extern int ydb_connect_has_result(struct YdbConnection* connection);
extern int ydb_connect_wait(struct YdbConnection* connection);
extern void ydb_connect_free(struct YdbConnection* connection);
extern struct YdbResult* ydb_query(struct YdbConnection* connection, char* query);
extern void ydb_result_free(struct YdbResult* res);
extern int ydb_result_has_result(struct YdbResult* res);
extern void ydb_result_wait(struct YdbResult* res);
extern int ydb_result_callback(struct YdbResult* res, YdbResultCallback *callback, YdbUserCustomData *data);
extern int ydb_result_has_errors(struct YdbResult* res);
extern int ydb_result_next_readset(struct YdbResult* res);
extern int ydb_result_next_row(struct YdbResult* res);
extern int ydb_result_read_first_field_text(struct YdbResult* res, void* dstBuffer, int dstBufferLen);
extern int ydb_check_linked_library();
