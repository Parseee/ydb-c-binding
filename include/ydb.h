#ifndef YDB_C_API_H
#define YDB_C_API_H

#include <stddef.h>
#include <stdint.h>

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
typedef int32_t ydb_status_t;
enum ydb_type_t : uint32_t;
typedef enum ydb_type_t
    ydb_type_t; // do i need to hide the implementation from user?

#define YDB_OK 0
#define YDB_ERR_GENERIC -1
#define YDB_ERR_CONNECTION -2
#define YDB_ERR_TIMEOUT -3
#define YDB_ERR_BAD_REQUEST -4
#define YDB_ERR_NOT_FOUND -5
#define YDB_ERR_INTERNAL -6
#define YDB_ERR_BUFFER_TOO_SMALL -7
#define YDB_ERR_NO_MORE_RESULTS -8
#define YDB_ERR_ALREADY_DONE -9

const char *ydb_last_error_message(void);

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

/* ============================================================
 * Driver Configuration & Lifecycle
 * ============================================================ */
YdbDriverConfig *ydb_driver_config_create(void);
void ydb_driver_config_free(YdbDriverConfig *cfg);

ydb_status_t ydb_driver_config_set_endpoint(YdbDriverConfig *cfg,
                                            const char *endpoint);
ydb_status_t ydb_driver_config_set_database(YdbDriverConfig *cfg,
                                            const char *database);
ydb_status_t ydb_driver_config_set_auth_token(YdbDriverConfig *cfg,
                                              const char *token);
/* Future: ydb_driver_config_set_tls_cert, _set_connection_pool_size, etc. */

YdbDriver *ydb_driver_create(const YdbDriverConfig *cfg);
ydb_status_t ydb_driver_start(YdbDriver *drv); /* non-blocking init */
ydb_status_t ydb_driver_wait_ready(YdbDriver *drv, int timeout_ms);
void ydb_driver_free(YdbDriver *drv); /* blocks until closed */

/* ============================================================
 * Query Parameters (for parameterized queries)
 * ============================================================ */
YdbQueryParams *ydb_query_params_create(void);
void ydb_query_params_free(YdbQueryParams *params);

// TODO: remove this
/* ============================================================
 * Params Builder — List<Struct<...>> and List<T>
 * ============================================================
 *
 * Usage for List<Struct<...>> (batch upsert):
 *
 *   YdbParamBuilder* b = ydb_params_begin_param(p, "$rows");
 *   ydb_params_begin_list(b);
 *     ydb_params_begin_struct(b);
 *       ydb_params_add_member_int64(b, "id",   1);
 *       ydb_params_add_member_utf8 (b, "name", "Alice");
 *     ydb_params_end_struct(b);
 *     ydb_params_begin_struct(b);
 *       ydb_params_add_member_int64(b, "id",   2);
 *       ydb_params_add_member_utf8 (b, "name", "Bob");
 *     ydb_params_end_struct(b);
 *   ydb_params_end_list(b);
 *   ydb_params_end_param(b);   // b is invalid after this
 */

YdbParamBuilder *ydb_params_begin_param(YdbQueryParams *p, const char *name);
ydb_status_t ydb_params_end_param(YdbParamBuilder *b);

ydb_status_t ydb_params_begin_list(YdbParamBuilder *b);
ydb_status_t ydb_params_add_list_item(YdbParamBuilder *b);
ydb_status_t ydb_params_end_list(YdbParamBuilder *b);

ydb_status_t ydb_params_begin_struct(YdbParamBuilder *b);
ydb_status_t ydb_params_end_struct(YdbParamBuilder *b);

ydb_status_t ydb_params_add_member_bool(YdbParamBuilder *b, const char *field,
                                        int v);
ydb_status_t ydb_params_add_member_int32(YdbParamBuilder *b, const char *field,
                                         int32_t v);
ydb_status_t ydb_params_add_member_uint32(YdbParamBuilder *b, const char *field,
                                          uint32_t v);
ydb_status_t ydb_params_add_member_int64(YdbParamBuilder *b, const char *field,
                                         int64_t v);
ydb_status_t ydb_params_add_member_uint64(YdbParamBuilder *b, const char *field,
                                          uint64_t v);
ydb_status_t ydb_params_add_member_float(YdbParamBuilder *b, const char *field,
                                         float v);
ydb_status_t ydb_params_add_member_double(YdbParamBuilder *b, const char *field,
                                          double v);
ydb_status_t ydb_params_add_member_utf8(YdbParamBuilder *b, const char *field,
                                        const char *v);
ydb_status_t ydb_params_add_member_bytes(YdbParamBuilder *b, const char *field,
                                         const void *data, size_t len);
ydb_status_t ydb_params_add_member_null(YdbParamBuilder *b, const char *field);
ydb_status_t ydb_params_add_member_decimal(YdbParamBuilder *b,
                                           const char *field, const char *value,
                                           uint8_t precision, uint8_t scale);

/* ============================================================
 * Scalar Parameters
 * ============================================================ */
ydb_status_t ydb_params_set_utf8(YdbQueryParams *p, const char *name,
                                 const char *value);
ydb_status_t ydb_params_set_int64(YdbQueryParams *p, const char *name,
                                  int64_t value);
ydb_status_t ydb_params_set_uint64(YdbQueryParams *p, const char *name,
                                   uint64_t value);
ydb_status_t ydb_params_set_double(YdbQueryParams *p, const char *name,
                                   double value);
ydb_status_t ydb_params_set_bool(YdbQueryParams *p, const char *name,
                                 int value);
ydb_status_t ydb_params_set_bytes(YdbQueryParams *p, const char *name,
                                  const void *data, size_t len);
ydb_status_t ydb_params_set_decimal(YdbQueryParams *p, const char *name,
                                    const char *value, uint8_t precision,
                                    uint8_t scale);

/* ============================================================
 * Access Services
 * ============================================================ */
typedef enum {
  YDB_TX_SERIALIZABLE_RW = 1,
  YDB_TX_ONLINE_RO = 2,
  YDB_TX_STALE_RO = 3,
  YDB_TX_SNAPSHOT_RO = 4,
  YDB_TX_SNAPSHOT_RW = 5, /* NEW: not in Table Service */
  YDB_TX_NONE = 6,        /* NEW: NoTx mode */
} ydb_tx_mode_t;

/* ============================================================
 * Table Service
 * ============================================================ */
YdbTableClient *ydb_table_client_create(YdbDriver *drv);
void ydb_table_client_free(YdbTableClient *tc);

ydb_status_t
ydb_table_execute_query(YdbTableClient *tc, const char *yql,
                        const YdbQueryParams *params, /* nullable */
                        YdbResultSets **out_results);
ydb_status_t ydb_table_execute_scheme(YdbTableClient *tc, const char *yql);

ydb_status_t ydb_table_begin_tx(YdbTableClient *, ydb_tx_mode_t,
                                YdbTableTransaction **);
ydb_status_t ydb_table_tx_execute(YdbTableTransaction *, const char *,
                                  const YdbQueryParams *, YdbResultSets **);
ydb_status_t ydb_table_tx_commit(YdbTableTransaction *);
ydb_status_t ydb_table_tx_rollback(YdbTableTransaction *);
void ydb_table_tx_free(YdbTableTransaction *);

/* ============================================================
 * Query Service
 * ============================================================ */
YdbQueryClient *ydb_query_client_create(YdbDriver *drv);
void ydb_query_client_free(YdbQueryClient *qc);

ydb_status_t ydb_query_begin_tx(YdbQueryClient *, ydb_tx_mode_t,
                                YdbQueryTransaction **);
ydb_status_t ydb_query_tx_execute(YdbQueryTransaction *, const char *,
                                  const YdbQueryParams *, YdbResultSets **);
ydb_status_t ydb_query_tx_commit(YdbQueryTransaction *);
ydb_status_t ydb_query_tx_rollback(YdbQueryTransaction *);
void ydb_query_tx_free(YdbQueryTransaction *);

/* ============================================================
 * Result Iteration
 * ============================================================ */
int ydb_result_sets_count(const YdbResultSets *rs);
YdbResultSet *ydb_result_sets_get(YdbResultSets *rs, int index);
void ydb_result_sets_free(YdbResultSets *rs);

int ydb_result_set_column_count(const YdbResultSet *rs);
const char *ydb_result_set_column_name(const YdbResultSet *rs, int col_index);
ydb_type_t ydb_result_set_column_type(const YdbResultSet *rs, int col_index);

int ydb_result_set_next_row(YdbResultSet *rs); // 0 if done

/* Value access (by column index for current row) */
ydb_status_t ydb_result_set_get_utf8(YdbResultSet *rs, int col,
                                     const char **out, size_t *out_len);
ydb_status_t ydb_result_set_get_int64(YdbResultSet *rs, int col, int64_t *out);
ydb_status_t ydb_result_set_get_uint64(YdbResultSet *rs, int col,
                                       uint64_t *out);
ydb_status_t ydb_result_set_get_double(YdbResultSet *rs, int col, double *out);
ydb_status_t ydb_result_set_get_bool(YdbResultSet *rs, int col, int *out);
ydb_status_t ydb_result_set_get_bytes(YdbResultSet *rs, int col,
                                      const void **out, size_t *out_len);
int ydb_result_set_is_null(YdbResultSet *rs, int col);

#ifdef __cplusplus
}
#endif

#endif /* YDB_C_API_H */