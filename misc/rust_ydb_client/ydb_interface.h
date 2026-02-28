#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct CPointer_Mutex_Arc_CallState_BindingQueryResult CPointer_Mutex_Arc_CallState_BindingQueryResult;

typedef struct CPointer_Mutex_Arc_CallState_Client CPointer_Mutex_Arc_CallState_Client;

typedef struct YdbConnection {
  struct CPointer_Mutex_Arc_CallState_Client client;
} YdbConnection;

typedef struct YdbResult {
  struct CPointer_Mutex_Arc_CallState_BindingQueryResult query_result;
} YdbResult;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

int ydb_check_linked_library(void);

struct YdbConnection *ydb_connect(const char *connection_string);

int ydb_connect_has_result(const struct YdbConnection *ydb_connection);

int ydb_connect_wait(const struct YdbConnection *ydb_connection);

void ydb_connect_free(struct YdbConnection *ydb_connection);

struct YdbResult *ydb_query(const struct YdbConnection *ydb_connection, const char *query);

void ydb_result_free(struct YdbResult *query_result);

int ydb_result_has_result(const struct YdbResult *query_result);

void ydb_result_wait(const struct YdbResult *query_result);

int ydb_result_has_errors(const struct YdbResult *query_result);

int ydb_result_next_readset(struct YdbResult *query_result);

int ydb_result_next_row(struct YdbResult *query_result);

int ydb_result_read_first_field_text(struct YdbResult *query_result,
                                     char *dst_buffer,
                                     int dst_buffer_len);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus
