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

/* details lifecycle */
void ydb_result_details_init(ydb_result_details_t *rd);
void ydb_result_details_reset(ydb_result_details_t *rd);
void ydb_result_details_free(ydb_result_details_t *rd);

/* details setters */
void ydb_result_details_set_status(ydb_result_details_t *rd, ydb_status_t code);
void ydb_result_details_set_retriable(ydb_result_details_t *rd, int is_retriable);
void ydb_result_details_set_message(ydb_result_details_t *rd, const char *msg);
void ydb_result_details_append_message(ydb_result_details_t *rd, const char *msg);
void ydb_result_details_set_context(ydb_result_details_t *rd, const char *ctx);

/* unified fail helper */
ydb_status_t ydb_result_details_fail(ydb_result_details_t *rd,
                                     ydb_status_t code,
                                     const char *msg);

#ifdef __cplusplus
}
#endif

#endif /* YDB_ERROR_H */
