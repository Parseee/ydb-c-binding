/**
 * C example: Create a "users" table and UPSERT a row.
 *
 * Mirrors the C++ example that uses ydb-cpp-sdk directly, but uses
 * the ydb-c-binding API declared in include/ydb.h.
 *
 * Build (dynamic, Go backend):
 *   gcc -o example examples/create_table_and_upsert.c \
 *       -Iinclude -Lgo/_obj -lydb -Wl,-rpath,go/_obj
 *
 * Build (dynamic, Rust backend):
 *   gcc -o example examples/create_table_and_upsert.c \
 *       -Iinclude -Lrust_ydb_client/target/release -lrust_ydb_client \
 *       -Wl,-rpath,rust_ydb_client/target/release
 */

#include <stdio.h>
#include <stdlib.h>
#include "include/ydb.h"

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static void check(ydb_status_t st, const char* op) {
    if (st != YDB_OK) {
        fprintf(stderr, "%s failed (code=%d): %s\n",
                op, st, ydb_last_error_message());
        exit((int)(-st));   /* positive exit code matching the C++ example */
    }
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(void) {
    ydb_status_t      st;
    YdbDriverConfig*  cfg     = NULL;
    YdbDriver*        drv     = NULL;
    YdbTableClient*   tc      = NULL;
    YdbQueryParams*   params  = NULL;

    /* -------------------------------------------------------------- */
    /* 1. Configure & create the driver                                */
    /* -------------------------------------------------------------- */
    cfg = ydb_driver_config_create();
    if (!cfg) {
        fprintf(stderr, "ydb_driver_config_create: out of memory\n");
        return 1;
    }

    check(ydb_driver_config_set_endpoint(cfg, "ydb-local:2136"), "set_endpoint");
    check(ydb_driver_config_set_database(cfg, "/local"),         "set_database");

    drv = ydb_driver_create(cfg);
    if (!drv) {
        fprintf(stderr, "ydb_driver_create failed: %s\n", ydb_last_error_message());
        ydb_driver_config_free(cfg);
        return 1;
    }
    /* config object is no longer needed after driver creation */
    ydb_driver_config_free(cfg);
    cfg = NULL;

    /* -------------------------------------------------------------- */
    /* 2. Create the Table client                                      */
    /* -------------------------------------------------------------- */
    tc = ydb_table_client_create(drv);
    if (!tc) {
        fprintf(stderr, "ydb_table_client_create failed: %s\n", ydb_last_error_message());
        ydb_driver_free(drv);
        return 1;
    }

    /* -------------------------------------------------------------- */
    /* 3. DDL – CREATE TABLE users                                     */
    /*    Mirrors: session.ExecuteSchemeQuery(...)                     */
    /* -------------------------------------------------------------- */
    st = ydb_table_execute_scheme(
        tc,
        "CREATE TABLE users ("
        "  id   Uint64,"
        "  name Utf8,"
        "  PRIMARY KEY (id)"
        ");"
    );
    if (st != YDB_OK) {
        fprintf(stderr, "DDL failed (code=%d): %s\n", st, ydb_last_error_message());
        ydb_table_client_free(tc);
        ydb_driver_free(drv);
        return 1;
    }
    printf("Table 'users' created (or already exists).\n");

    /* -------------------------------------------------------------- */
    /* 4. DML – UPSERT a row                                           */
    /*    Mirrors: session.ExecuteDataQuery("UPSERT ...", params)      */
    /* -------------------------------------------------------------- */
    params = ydb_query_params_create();
    if (!params) {
        fprintf(stderr, "ydb_query_params_create: out of memory\n");
        ydb_table_client_free(tc);
        ydb_driver_free(drv);
        return 2;
    }

    check(ydb_query_params_set_uint64(params, "$id",   42),      "params_set_uint64 $id");
    check(ydb_query_params_set_utf8  (params, "$name", "Alice"), "params_set_utf8 $name");

    st = ydb_table_execute_query(
        tc,
        "UPSERT INTO users (id, name) VALUES ($id, $name)",
        params,
        NULL   /* no result sets expected for UPSERT */
    );
    ydb_query_params_free(params);
    params = NULL;

    if (st != YDB_OK) {
        fprintf(stderr, "UPSERT failed (code=%d): %s\n", st, ydb_last_error_message());
        ydb_table_client_free(tc);
        ydb_driver_free(drv);
        return 2;
    }
    printf("UPSERT succeeded: id=42, name=Alice\n");

    /* -------------------------------------------------------------- */
    /* 5. Cleanup  (mirrors: driver.Stop(true))                        */
    /* -------------------------------------------------------------- */
    ydb_table_client_free(tc);
    ydb_driver_free(drv);   /* internally calls driver->Stop(true) */

    return 0;
}