#ifndef YDB_C_API_H
#define YDB_C_API_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Version & Library Info
 * ============================================================ */
#define YDB_C_API_VERSION_MAJOR 1
#define YDB_C_API_VERSION_MINOR 0
#define YDB_C_API_VERSION_PATCH 0

int ydb_version_major(void);
int ydb_version_minor(void);
int ydb_version_patch(void);

/* ============================================================
 * Error Handling
 * ============================================================ */
typedef int32_t ydb_status_t;

#define YDB_OK                   0
#define YDB_ERR_GENERIC         -1
#define YDB_ERR_CONNECTION      -2
#define YDB_ERR_TIMEOUT         -3
#define YDB_ERR_BAD_REQUEST     -4
#define YDB_ERR_NOT_FOUND       -5
#define YDB_ERR_INTERNAL        -6
#define YDB_ERR_BUFFER_TOO_SMALL -7
#define YDB_ERR_NO_MORE_RESULTS -8
#define YDB_ERR_ALREADY_DONE    -9

/* Thread-local last error message (like errno + strerror) */
const char* ydb_last_error_message(void);

/* ============================================================
 * Opaque Handle Types
 * ============================================================ */
typedef struct YdbDriver        YdbDriver;
typedef struct YdbDriverConfig  YdbDriverConfig;
typedef struct YdbTableClient   YdbTableClient;
typedef struct YdbSession       YdbSession;
typedef struct YdbTransaction   YdbTransaction;
typedef struct YdbResultSets    YdbResultSets;
typedef struct YdbResultSet     YdbResultSet;
typedef struct YdbRow           YdbRow;
typedef struct YdbValue         YdbValue;
typedef struct YdbQueryParams   YdbQueryParams;

/* ============================================================
 * Driver Configuration & Lifecycle
 * ============================================================ */
YdbDriverConfig* ydb_driver_config_create(void);
void             ydb_driver_config_free(YdbDriverConfig* cfg);

ydb_status_t ydb_driver_config_set_endpoint(YdbDriverConfig* cfg, const char* endpoint);
ydb_status_t ydb_driver_config_set_database(YdbDriverConfig* cfg, const char* database);
ydb_status_t ydb_driver_config_set_auth_token(YdbDriverConfig* cfg, const char* token);
/* Future: ydb_driver_config_set_tls_cert, _set_connection_pool_size, etc. */

YdbDriver*   ydb_driver_create(const YdbDriverConfig* cfg);
ydb_status_t ydb_driver_start(YdbDriver* drv);         /* non-blocking init */
ydb_status_t ydb_driver_wait_ready(YdbDriver* drv, int timeout_ms);
void         ydb_driver_free(YdbDriver* drv);           /* blocks until closed */

/* ============================================================
 * Table Client
 * ============================================================ */
YdbTableClient* ydb_table_client_create(YdbDriver* drv);
void            ydb_table_client_free(YdbTableClient* tc);

/* ============================================================
 * Query Parameters (for parameterized queries)
 * ============================================================ */
YdbQueryParams* ydb_query_params_create(void);
void            ydb_query_params_free(YdbQueryParams* params);

ydb_status_t ydb_query_params_set_utf8(YdbQueryParams* p, const char* name, const char* value);
ydb_status_t ydb_query_params_set_int64(YdbQueryParams* p, const char* name, int64_t value);
ydb_status_t ydb_query_params_set_uint64(YdbQueryParams* p, const char* name, uint64_t value);
ydb_status_t ydb_query_params_set_double(YdbQueryParams* p, const char* name, double value);
ydb_status_t ydb_query_params_set_bool(YdbQueryParams* p, const char* name, int value);
ydb_status_t ydb_query_params_set_bytes(YdbQueryParams* p, const char* name,
                                         const void* data, size_t len);
/* Future: _set_json, _set_datetime, _set_optional_*, etc. */

/* ============================================================
 * Simple Query Execution (auto-session, auto-retry)
 * ============================================================ */
ydb_status_t ydb_table_execute_query(
    YdbTableClient* tc,
    const char* yql,
    const YdbQueryParams* params,  /* nullable */
    YdbResultSets** out_results
);

/* DDL (no result sets expected) */
ydb_status_t ydb_table_execute_scheme(
    YdbTableClient* tc,
    const char* yql
);

/* ============================================================
 * Explicit Transaction Control
 * ============================================================ */

/* Transaction modes */
#define YDB_TX_SERIALIZABLE_RW   1
#define YDB_TX_ONLINE_RO         2
#define YDB_TX_STALE_RO          3
#define YDB_TX_SNAPSHOT_RO       4

ydb_status_t ydb_table_begin_tx(YdbTableClient* tc, int tx_mode, YdbTransaction** out_tx);
ydb_status_t ydb_tx_execute(YdbTransaction* tx, const char* yql,
                             const YdbQueryParams* params, YdbResultSets** out_results);
ydb_status_t ydb_tx_commit(YdbTransaction* tx);
ydb_status_t ydb_tx_rollback(YdbTransaction* tx);
void         ydb_tx_free(YdbTransaction* tx);

/* ============================================================
 * Result Iteration
 * ============================================================ */
int          ydb_result_sets_count(const YdbResultSets* rs);
YdbResultSet* ydb_result_sets_get(YdbResultSets* rs, int index);
void         ydb_result_sets_free(YdbResultSets* rs);

/* Column info */
int          ydb_result_set_column_count(const YdbResultSet* rs);
const char*  ydb_result_set_column_name(const YdbResultSet* rs, int col_index);
int          ydb_result_set_column_type(const YdbResultSet* rs, int col_index);

/* Row iteration */
int          ydb_result_set_next_row(YdbResultSet* rs);  /* returns 1 if row available, 0 if done */

/* Value access (by column index for current row) */
ydb_status_t ydb_result_set_get_utf8(YdbResultSet* rs, int col, const char** out, size_t* out_len);
ydb_status_t ydb_result_set_get_int64(YdbResultSet* rs, int col, int64_t* out);
ydb_status_t ydb_result_set_get_uint64(YdbResultSet* rs, int col, uint64_t* out);
ydb_status_t ydb_result_set_get_double(YdbResultSet* rs, int col, double* out);
ydb_status_t ydb_result_set_get_bool(YdbResultSet* rs, int col, int* out);
ydb_status_t ydb_result_set_get_bytes(YdbResultSet* rs, int col, const void** out, size_t* out_len);
int          ydb_result_set_is_null(YdbResultSet* rs, int col);

/* ============================================================
 * YDB Type IDs (for column_type)
 * ============================================================ */
#define YDB_TYPE_BOOL     1
#define YDB_TYPE_INT32    2
#define YDB_TYPE_UINT32   3
#define YDB_TYPE_INT64    4
#define YDB_TYPE_UINT64   5
#define YDB_TYPE_FLOAT    6
#define YDB_TYPE_DOUBLE   7
#define YDB_TYPE_UTF8     8
#define YDB_TYPE_BYTES    9
#define YDB_TYPE_JSON    10
#define YDB_TYPE_DATE    11
#define YDB_TYPE_DATETIME 12
#define YDB_TYPE_TIMESTAMP 13

#ifdef __cplusplus
}
#endif

#endif /* YDB_C_API_H */