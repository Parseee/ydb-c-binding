#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "../c_interface/ydb_interface.h"

int main(){
    int res;
    res = ydb_check_linked_library();
    printf("check %d\n", res);

    char *connectionString = "grpc://localhost:2136/local";

    YdbConnection *connection = ydb_connect(connectionString);

    int hasConnectedWithoutWait = ydb_connect_has_result(connection);
    printf("hasConnectedWithoutWait: %d\n", hasConnectedWithoutWait);

    int connectErr = ydb_connect_wait(connection);
    if (connectErr) {
        printf("connect failed: %d\n", connectErr);
        return 1;
    }
    printf("connect success\n");

    char *query = "SELECT CAST(111 + 234 AS Utf8)";

    YdbResult *result = ydb_query(connection, query);

    int hasResultWithoutWait = ydb_result_has_result(result);
    printf("hasResultWithoutWait: %d\n", hasResultWithoutWait);

    ydb_result_wait(result);
    if (ydb_result_has_errors(result)){
        ydb_result_free(result);
        ydb_connect_free(connection);
        printf("failed to do result");
        return 2;
    }

    ydb_result_next_readset(result);
    ydb_result_next_row(result);

    const int bufLen = 1024;
    char *buf = malloc(bufLen);

    strcpy(buf, "BAD");

    int readErr = ydb_result_read_first_field_text(result, buf, bufLen);
    if(readErr) {
        ydb_result_has_errors(result);
    } else {
        printf("result: '%s'", buf);
    }

    free(buf);

    ydb_result_free(result);
    ydb_connect_free(connection);

    return 0;
}
