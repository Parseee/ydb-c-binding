#ifndef YDB_ERROR_H
#define YDB_ERROR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t ydb_status_t;

/* FFI-safe per-call result object */
typedef struct ydb_result_details_t {
  ydb_status_t code;

  char *message;
  size_t message_len;
  size_t message_cap;

  char *context;
  size_t context_len;
  size_t context_cap;
} ydb_result_details_t;

/* logger */
void ydb_result_details_print(const char *message);

/* thread-local legacy error */
const char *ydb_last_error_message(void);
void ydb_clear_last_error(void);

/* details lifecycle */
void ydb_result_details_init(ydb_result_details_t *d);
void ydb_result_details_reset(ydb_result_details_t *d);
void ydb_result_details_free(ydb_result_details_t *d);

/* details setters */
void ydb_result_details_set_status(ydb_result_details_t *d, ydb_status_t code);
void ydb_result_details_set_retriable(ydb_result_details_t *d, int is_retriable);
void ydb_result_details_set_message(ydb_result_details_t *d, const char *msg);
void ydb_result_details_append_message(ydb_result_details_t *d, const char *msg);
void ydb_result_details_set_context(ydb_result_details_t *d, const char *ctx);

/* unified fail helper */
ydb_status_t ydb_result_details_fail(ydb_result_details_t *d,
                                     ydb_status_t code,
                                     int is_retriable,
                                     const char *msg);

#ifdef __cplusplus
}
#endif

#endif /* YDB_ERROR_H */