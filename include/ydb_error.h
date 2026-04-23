#ifndef YDB_ERROR_H
#define YDB_ERROR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t ydb_status_t;

typedef struct YdbResultDetails YdbResultDetails;

void ydb_result_details_print(const char *message);

void ydb_result_details_init(YdbResultDetails *rd);
void ydb_result_details_reset(YdbResultDetails *rd);
void ydb_result_details_free(YdbResultDetails *rd);

const char *get_message(const YdbResultDetails *d);

ydb_status_t ydb_result_details_fail(YdbResultDetails *rd, ydb_status_t code,
                                     const char *msg);
int ydb_is_status_retriable(ydb_status_t sdk_status_code);

#ifdef __cplusplus
}
#endif

#endif /* YDB_ERROR_H */
