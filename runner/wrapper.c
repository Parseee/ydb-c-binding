#include "include/ydb.h"
#include <stdio.h>
#include <stdlib.h>

static const char *details_message(const ydb_result_details_t *rd) {
  if (!rd || !rd->message || rd->message_len == 0) {
    return "no details";
  }
  return rd->message;
}

static void reset_details(ydb_result_details_t *rd) {
  if (rd) {
    ydb_result_details_reset(rd);
  }
}

static void check_status(ydb_status_t st, const char *op,
                         const ydb_result_details_t *rd) {
  if (st != YDB_OK) {
    fprintf(stderr, "%s failed (code=%d): %s\n", op, st, details_message(rd));
    exit((int)(-st));
  }
}

int main(void) {
  ydb_status_t st;
  ydb_result_details_t rd;
  YdbDriverConfig *cfg = NULL;
  YdbDriver *drv = NULL;
  YdbQueryClient *qc = NULL;
  YdbQueryTransaction *tx = NULL;
  YdbQueryParams *params = NULL;

  ydb_result_details_init(&rd);

  cfg = ydb_driver_config_create(&rd);
  if (!cfg) {
    fprintf(stderr, "ydb_driver_config_create failure\n");
    ydb_result_details_free(&rd);
    return 1;
  }

  reset_details(&rd);
  st = ydb_driver_config_set_endpoint(cfg, "ydb-local:2136", &rd);
  check_status(st, "set_endpoint", &rd);

  reset_details(&rd);
  st = ydb_driver_config_set_database(cfg, "/local", &rd);
  check_status(st, "set_database", &rd);

  drv = ydb_driver_create(cfg, &rd);
  ydb_driver_config_free(cfg);
  cfg = NULL;
  if (!drv) {
    fprintf(stderr, "ydb_driver_create failed\n");
    ydb_result_details_free(&rd);
    return 1;
  }

  reset_details(&rd);
  qc = ydb_query_client_create(drv, &rd);
  if (!qc) {
    fprintf(stderr, "ydb_query_client_create failed: %s\n",
            details_message(&rd));
    ydb_driver_free(drv);
    ydb_result_details_free(&rd);
    return 1;
  }

  reset_details(&rd);

  // st = ydb_query_begin_tx(qc, YDB_TX_NONE, &tx, &rd);
  // check_status(st, "begin_tx (DDL)", &rd);
  // reset_details(&rd);
  // st = ydb_query_tx_execute(tx,
  //                           "CREATE TABLE users ("
  //                           "  id   Uint64,"
  //                           "  name Utf8,"
  //                           "  PRIMARY KEY (id)"
  //                           ");",
  //                           NULL, NULL, &rd);
  // check_status(st, "execute DDL", &rd);

  // reset_details(&rd);
  // st = ydb_query_tx_commit(tx, &rd);
  // check_status(st, "commit DDL", &rd);
  // ydb_query_tx_free(tx, &rd);
  // tx = NULL;
  // printf("Table 'users' created (or already exists).\n");

  ydb_query_execute(qc,
                    "CREATE TABLE users ("
                    "  id   Uint64,"
                    "  name Utf8,"
                    "  PRIMARY KEY (id)"
                    ");",
                    YDB_TX_NONE, NULL, NULL, &rd);

  reset_details(&rd);
  params = ydb_query_params_create(&rd);
  if (!params) {
    fprintf(stderr, "ydb_query_params_create failed: %s\n",
            details_message(&rd));
    ydb_query_client_free(qc);
    ydb_driver_free(drv);
    ydb_result_details_free(&rd);
    return 2;
  }

  int32_t id = 52;
  const char *name = "bebebe bububu";

  reset_details(&rd);
  st = ydb_params_set_uint64(params, "$id", id, &rd);
  check_status(st, "params_set_uint64 $id", &rd);

  reset_details(&rd);
  st = ydb_params_set_utf8(params, "$name", name, &rd);
  check_status(st, "params_set_utf8 $name", &rd);

  reset_details(&rd);
  st = ydb_query_begin_tx(qc, YDB_TX_SERIALIZABLE_RW, &tx, &rd);
  check_status(st, "begin_tx (UPSERT)", &rd);

  reset_details(&rd);
  st = ydb_query_tx_execute(tx,
                            "UPSERT INTO users (id, name) VALUES ($id, $name)",
                            params, NULL, &rd);
  check_status(st, "execute UPSERT", &rd);

  reset_details(&rd);
  st = ydb_query_tx_commit(tx, &rd);
  check_status(st, "commit UPSERT", &rd);
  ydb_query_tx_free(tx, &rd);
  tx = NULL;

  ydb_query_params_free(params, &rd);
  params = NULL;

  printf("UPSERT succeeded: id=%d, name=%s\n", id, name);

  ydb_query_client_free(qc);
  ydb_driver_free(drv);
  ydb_result_details_free(&rd);

  return 0;
}
