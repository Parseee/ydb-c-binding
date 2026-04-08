#ifndef YDB_C_API_H
#define YDB_C_API_H

#include <stddef.h>
#include <stdint.h>

#include "ydb_error.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Version & Library Info
 * ============================================================ */

typedef struct Version YdbVersionT;
const char *ydb_get_version();

/* ============================================================
 * Error Handling
 * ============================================================ */

typedef enum ydb_error_t {
  YDB_OK = 0,
  YDB_ERR_GENERIC = -1,
  YDB_ERR_CONNECTION = -2,
  YDB_ERR_TIMEOUT = -3,
  YDB_ERR_BAD_REQUEST = -4,
  YDB_ERR_NOT_FOUND = -5,
  YDB_ERR_INTERNAL = -6,
  YDB_ERR_BUFFER_TOO_SMALL = -7,
  YDB_ERR_NO_MORE_RESULTS = -8,
  YDB_ERR_ALREADY_DONE = -9
} ydb_error_t;

typedef enum ydb_type_t {
  YDB_TYPE_BOOL = 0x0006,
  YDB_TYPE_INT8 = 0x0007,
  YDB_TYPE_UINT8 = 0x0005,
  YDB_TYPE_INT16 = 0x0008,
  YDB_TYPE_UINT16 = 0x0009,
  YDB_TYPE_INT32 = 0x0001,
  YDB_TYPE_UINT32 = 0x0002,
  YDB_TYPE_INT64 = 0x0003,
  YDB_TYPE_UINT64 = 0x0004,
  YDB_TYPE_FLOAT = 0x0021,
  YDB_TYPE_DOUBLE = 0x0020,
  YDB_TYPE_DATE = 0x0030,
  YDB_TYPE_DATETIME = 0x0031,
  YDB_TYPE_TIMESTAMP = 0x0032,
  YDB_TYPE_INTERVAL = 0x0033,
  YDB_TYPE_BYTES = 0x1001,
  YDB_TYPE_UTF8 = 0x1200,
  YDB_TYPE_JSON = 0x1202,
  YDB_TYPE_UUID = 0x1203,
  YDB_TYPE_JSON_DOC = 0x1204,
  YDB_TYPE_OPTIONAL = 0x0100,
  YDB_TYPE_UNKNOWN = 0x0000,
} ydb_type_t;

typedef struct YdbDriver YdbDriver;
typedef struct YdbDriverConfig YdbDriverConfig;
typedef struct YdbTableClient YdbTableClient;
typedef struct YdbQueryClient YdbQueryClient;
typedef struct YdbSession YdbSession;
typedef struct YdbTableTransaction YdbTableTransaction;
typedef struct YdbQueryTransaction YdbQueryTransaction;
typedef struct YdbResultSets YdbResultSets;
typedef struct YdbResultSet YdbResultSet;
typedef struct YdbRow YdbRow;
typedef struct YdbValue YdbValue;
typedef struct YdbQueryParams YdbQueryParams;
typedef struct YdbParamBuilder YdbParamBuilder;
typedef struct YdbErrorLogger YdbErrorLogger;
typedef struct YdbQueryRetrySettings YdbQueryRetrySettings;
typedef struct YdbResultDetails YdbResultDetails;

/* ============================================================
 * Driver Configuration & Lifecycle
 * ============================================================ */
YdbDriverConfig *ydb_driver_config_create(YdbResultDetails *rd);
void ydb_driver_config_free(YdbDriverConfig *cfg);

ydb_status_t ydb_driver_config_set_endpoint(YdbDriverConfig *cfg,
                                            const char *endpoint,
                                            YdbResultDetails *rd);
ydb_status_t ydb_driver_config_set_database(YdbDriverConfig *cfg,
                                            const char *database,
                                            YdbResultDetails *rd);
ydb_status_t ydb_driver_config_set_auth_token(YdbDriverConfig *cfg,
                                              const char *token,
                                              YdbResultDetails *rd);
/* Future: ydb_driver_config_set_tls_cert, _set_connection_pool_size, etc. */

YdbDriver *ydb_driver_create(const YdbDriverConfig *cfg, YdbResultDetails *rd);
void ydb_driver_free(YdbDriver *drv); /* blocks until closed */
ydb_status_t ydb_driver_start(YdbDriver *drv,
                              YdbResultDetails *rd); /* non-blocking init */
ydb_status_t ydb_driver_wait_ready(YdbDriver *drv, int timeout_ms,
                                   YdbResultDetails *rd);

/* ============================================================
 * Query Parameters (for parameterized queries)
 * ============================================================ */
YdbQueryParams *ydb_query_params_create(YdbResultDetails *rd);
void ydb_query_params_free(YdbQueryParams *params, YdbResultDetails *rd);

YdbParamBuilder *ydb_params_begin_param(YdbQueryParams *p, const char *name,
                                        YdbResultDetails *rd);
ydb_status_t ydb_params_end_param(YdbParamBuilder *b, YdbResultDetails *rd);

ydb_status_t ydb_params_begin_list(YdbParamBuilder *b, YdbResultDetails *rd);
ydb_status_t ydb_params_add_list_item(YdbParamBuilder *b, YdbResultDetails *rd);
ydb_status_t ydb_params_end_list(YdbParamBuilder *b, YdbResultDetails *rd);

ydb_status_t ydb_params_begin_struct(YdbParamBuilder *b, YdbResultDetails *rd);
ydb_status_t ydb_params_end_struct(YdbParamBuilder *b, YdbResultDetails *rd);

ydb_status_t ydb_params_add_member_bool(YdbParamBuilder *b, const char *field,
                                        int v, YdbResultDetails *rd);
ydb_status_t ydb_params_add_member_int32(YdbParamBuilder *b, const char *field,
                                         int32_t v, YdbResultDetails *rd);
ydb_status_t ydb_params_add_member_uint32(YdbParamBuilder *b, const char *field,
                                          uint32_t v, YdbResultDetails *rd);
ydb_status_t ydb_params_add_member_int64(YdbParamBuilder *b, const char *field,
                                         int64_t v, YdbResultDetails *rd);
ydb_status_t ydb_params_add_member_uint64(YdbParamBuilder *b, const char *field,
                                          uint64_t v, YdbResultDetails *rd);
ydb_status_t ydb_params_add_member_float(YdbParamBuilder *b, const char *field,
                                         float v, YdbResultDetails *rd);
ydb_status_t ydb_params_add_member_double(YdbParamBuilder *b, const char *field,
                                          double v, YdbResultDetails *rd);
ydb_status_t ydb_params_add_member_utf8(YdbParamBuilder *b, const char *field,
                                        const char *v, YdbResultDetails *rd);
ydb_status_t ydb_params_add_member_bytes(YdbParamBuilder *b, const char *field,
                                         const void *data, size_t len,
                                         YdbResultDetails *rd);
ydb_status_t ydb_params_add_member_null(YdbParamBuilder *b, const char *field,
                                        YdbResultDetails *rd);

/* ============================================================
 * Scalar Parameters
 * ============================================================ */
ydb_status_t ydb_params_set_utf8(YdbQueryParams *p, const char *name,
                                 const char *value, YdbResultDetails *rd);
ydb_status_t ydb_params_set_int64(YdbQueryParams *p, const char *name,
                                  int64_t value, YdbResultDetails *rd);
ydb_status_t ydb_params_set_uint64(YdbQueryParams *p, const char *name,
                                   uint64_t value, YdbResultDetails *rd);
ydb_status_t ydb_params_set_double(YdbQueryParams *p, const char *name,
                                   double value, YdbResultDetails *rd);
ydb_status_t ydb_params_set_bool(YdbQueryParams *p, const char *name, int value,
                                 YdbResultDetails *rd);
ydb_status_t ydb_params_set_bytes(YdbQueryParams *p, const char *name,
                                  const void *data, size_t len,
                                  YdbResultDetails *rd);

/* ============================================================
 * Access Services
 * ============================================================ */
typedef enum {
  YDB_TX_SERIALIZABLE_RW = 1,
  YDB_TX_ONLINE_RO = 2,
  YDB_TX_STALE_RO = 3,
  YDB_TX_SNAPSHOT_RO = 4,
  YDB_TX_SNAPSHOT_RW = 5,
  YDB_TX_NONE = 6,
} ydb_tx_mode_t;

/* ============================================================
 * Query Service
 * ============================================================ */
YdbQueryClient *ydb_query_client_create(YdbDriver *drv, YdbResultDetails *rd);
void ydb_query_client_free(YdbQueryClient *qc);

// for DDL commands
ydb_status_t
ydb_query_execute(YdbQueryClient *qc, const char *yql, ydb_tx_mode_t tx_mode,
                  const YdbQueryParams *params, YdbResultSets **out_results,
                  YdbQueryRetrySettings *rs, YdbResultDetails *result_details);

ydb_status_t ydb_query_begin_tx(YdbQueryClient *, ydb_tx_mode_t,
                                YdbQueryTransaction **, YdbResultDetails *rd);
ydb_status_t ydb_query_tx_execute(YdbQueryTransaction *, const char *,
                                  const YdbQueryParams *, YdbResultSets **,
                                  YdbResultDetails *rd);
ydb_status_t ydb_query_tx_commit(YdbQueryTransaction *, YdbResultDetails *rd);
ydb_status_t ydb_query_tx_rollback(YdbQueryTransaction *, YdbResultDetails *rd);
void ydb_query_tx_free(YdbQueryTransaction *, YdbResultDetails *rd);

/* ============================================================
 * Result Iteration
 * ============================================================ */
int ydb_resultsets_count(const YdbResultSets *rs, YdbResultDetails *rd);
YdbResultSet *ydb_resultsets_get(YdbResultSets *rs, int index,
                                 YdbResultDetails *rd);
void ydb_resultsets_free(YdbResultSets *rs, YdbResultDetails *rd);

int ydb_resultset_column_count(const YdbResultSet *rs, YdbResultDetails *rd);
const char *ydb_resultset_column_name(const YdbResultSet *rs, int col_index,
                                      YdbResultDetails *rd);
ydb_type_t ydb_resultset_column_type(const YdbResultSet *rs, int col_index,
                                     YdbResultDetails *rd);

int ydb_resultset_next_row(YdbResultSet *rs,
                           YdbResultDetails *rd); // 0 if done

ydb_status_t ydb_resultset_get_utf8(YdbResultSet *rs, int col, const char **out,
                                    size_t *out_len, YdbResultDetails *rd);
ydb_status_t ydb_resultset_get_int64(YdbResultSet *rs, int col, int64_t *out,
                                     YdbResultDetails *rd);
ydb_status_t ydb_resultset_get_uint64(YdbResultSet *rs, int col, uint64_t *out,
                                      YdbResultDetails *rd);
ydb_status_t ydb_resultset_get_double(YdbResultSet *rs, int col, double *out,
                                      YdbResultDetails *rd);
ydb_status_t ydb_resultset_get_bool(YdbResultSet *rs, int col, int *out,
                                    YdbResultDetails *rd);
ydb_status_t ydb_resultset_get_bytes(YdbResultSet *rs, int col,
                                     const void **out, size_t *out_len,
                                     YdbResultDetails *rd);
int ydb_resultset_is_null(YdbResultSet *rs, int col, YdbResultDetails *rd);

#ifdef __cplusplus
}
#endif

#endif /* YDB_C_API_H */
