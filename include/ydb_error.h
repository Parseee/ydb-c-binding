#ifndef YDB_ERROR_H
#define YDB_ERROR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t ydb_status_t;

typedef struct YdbResultDetails YdbResultDetails;

int ydb_result_details_init(YdbResultDetails **rd);
void ydb_result_details_reset(YdbResultDetails *rd);
void ydb_result_details_free(YdbResultDetails *rd);

const char *get_message(const YdbResultDetails *d);

#ifdef __cplusplus
}
#endif

#endif /* YDB_ERROR_H */
