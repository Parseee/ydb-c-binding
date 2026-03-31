#include "include/ydb.h"
#include <stdio.h>
#include <stdlib.h>

static void check(ydb_status_t st, const char *op) {
  if (st != YDB_OK) {
    fprintf(stderr, "%s failed (code=%d)\n", op, st);
    exit((int)(-st));
  }
}

int main(void) {
  ydb_status_t st;
  YdbDriverConfig *cfg = NULL;
  YdbDriver *drv = NULL;
  YdbTableClient *tc = NULL;
  YdbQueryParams *params = NULL;

  cfg = ydb_driver_config_create();
  if (!cfg) {
    fprintf(stderr, "ydb_driver_config_create failure\n");
    return 1;
  }

  // check(ydb_driver_config_set_endpoint(cfg, "ydb-local:2136"), "set_endpoint");
  // check(ydb_driver_config_set_database(cfg, "/local"), "set_database");

  // drv = ydb_driver_create(cfg);
  // if (!drv) {
  //   fprintf(stderr, "ydb_driver_create failed: %s\n", ydb_last_error_message());
  //   ydb_driver_config_free(cfg);
  //   return 1;
  // }
  // ydb_driver_config_free(cfg);
  // cfg = NULL;

  // tc = ydb_table_client_create(drv);
  // if (!tc) {
  //   fprintf(stderr, "ydb_table_client_create failed: %s\n",
  //           ydb_last_error_message());
  //   ydb_driver_free(drv);
  //   return 1;
  // }

  // st = ydb_table_execute_scheme(tc, "CREATE TABLE users ("
  //                                   "  id   Uint64,"
  //                                   "  name Utf8,"
  //                                   "  PRIMARY KEY (id)"
  //                                   ");");
  // if (st != YDB_OK) {
  //   fprintf(stderr, "DDL failed (code=%d): %s\n", st, ydb_last_error_message());
  //   ydb_table_client_free(tc);
  //   ydb_driver_free(drv);
  //   return 1;
  // }
  // printf("Table 'users' created (or already exists).\n");

  // params = ydb_query_params_create();
  // if (!params) {
  //   fprintf(stderr, "ydb_query_params_create: out of memory\n");
  //   ydb_table_client_free(tc);
  //   ydb_driver_free(drv);
  //   return 2;
  // }

  // int32_t id = 52;
  // const char *name = "bebebe bububu";

  // check(ydb_params_set_uint64(params, "$id", id),
  //       "params_set_uint64 $id");
  // check(ydb_params_set_utf8(params, "$name", name),
  //       "params_set_utf8 $name");

  // st = ydb_table_execute_query(
  //     tc, "UPSERT INTO users (id, name) VALUES ($id, $name)", params, NULL);
  // ydb_query_params_free(params);
  // params = NULL;

  // if (st != YDB_OK) {
  //   fprintf(stderr, "UPSERT failed (code=%d): %s\n", st,
  //           ydb_last_error_message());
  //   ydb_table_client_free(tc);
  //   ydb_driver_free(drv);
  //   return 2;
  // }
  // printf("UPSERT succeeded: id=%d, name=%s\n", id, name);

  // ydb_table_client_free(tc);
  // ydb_driver_free(drv);

  return 0;
}