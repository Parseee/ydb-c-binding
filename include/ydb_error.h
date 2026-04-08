#ifndef YDB_ERROR_H
#define YDB_ERROR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t ydb_status_t;

typedef struct ydb_result_details_t {
  ydb_status_t code;

  char *message;
  size_t message_len;
  size_t message_cap;

  char *context;
  size_t context_len;
  size_t context_cap;
} YdbResultDetails; // change to ydb context
// store info about detqails and pointer to context
// context must be constant and set here
// context is not request context
// - context of TX
// - part of transaction (begin_tx(..., retry settings*, ...)) 

void ydb_result_details_print(const char *message);

void ydb_result_details_init(YdbResultDetails *rd);
void ydb_result_details_reset(YdbResultDetails *rd);
void ydb_result_details_free(YdbResultDetails *rd);

void ydb_result_details_set_status(YdbResultDetails *rd, ydb_status_t code); // must be non user accessible
void ydb_result_details_set_message(YdbResultDetails *rd, const char *msg); // must be non user accessible
void ydb_result_details_append_message(YdbResultDetails *rd, const char *msg); // must be non user accessible
void ydb_result_details_set_context(YdbResultDetails *rd, const char *ctx); // must be non user accessible

ydb_status_t ydb_result_details_fail(YdbResultDetails *rd,
                                     ydb_status_t code,
                                     const char *msg);

#ifdef __cplusplus
}
#endif

#endif /* YDB_ERROR_H */
