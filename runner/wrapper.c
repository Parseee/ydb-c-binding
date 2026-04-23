#include "include/ydb.h"
#include "ydb_error.h"
#include <stdio.h>
#include <stdlib.h>

static const char *details_message(const YdbResultDetails *rd) {
  if (!rd) {
    return "no details";
  }
  return get_message(rd);
}

static void check_status(ydb_status_t st, const char *op,
                         const YdbResultDetails *rd) {
  if (st != YDB_OK) {
    fprintf(stderr, "%s failed (code=%d): %s\n", op, st, details_message(rd));
    exit((int)(-st));
  }
}

int main(void) {
  ydb_status_t st;
  YdbResultDetails *rd = NULL;
  YdbDriverConfig *cfg = NULL;
  YdbDriver *drv = NULL;
  YdbQueryClient *qc = NULL;
  YdbQueryTransaction *tx = NULL;
  YdbQueryParams *params = NULL;
  YdbQueryRetrySettings *rs = NULL;

  if (ydb_result_details_init(rd) != 0) {
    fprintf(stderr, "result details creation failed\n");
    return -1;
  }

  cfg = ydb_driver_config_create(rd);
  if (!cfg) {
    fprintf(stderr, "ydb_driver_config_create failure\n");
    return 1;
  }

  st = ydb_driver_config_set_endpoint(cfg, "ydb-local:2136", rd);
  check_status(st, "set_endpoint", rd);

  st = ydb_driver_config_set_database(cfg, "/local", rd);
  check_status(st, "set_database", rd);

  drv = ydb_driver_create(cfg, rd);
  ydb_driver_config_free(cfg);
  cfg = NULL;
  if (!drv) {
    fprintf(stderr, "ydb_driver_create failed\n");
    return 1;
  }

  qc = ydb_query_client_create(drv, rd);
  if (!qc) {
    fprintf(stderr, "ydb_query_client_create failed: %s\n",
            details_message(rd));
    ydb_driver_free(drv);
    return 1;
  }

  rs = ydb_query_retry_settings_create(5, 100, rd);
  if (!rs) {
    fprintf(stderr, "ydb_query_retry_settings_create failed: %s\n",
            details_message(rd));
    ydb_query_client_free(qc);
    ydb_driver_free(drv);
    return 1;
  }

  st = ydb_query_NOtx_execute(qc,
                              "CREATE TABLE IF NOT EXISTS users ("
                              "  key   Uint64,"
                              "  value Utf8,"
                              "  PRIMARY KEY (key)"
                              ");",
                              NULL, NULL, rd);
  check_status(st, "execute DDL", rd);

  params = ydb_query_params_create(rd);
  if (!params) {
    fprintf(stderr, "ydb_query_params_create failed: %s\n",
            details_message(rd));
    ydb_query_client_free(qc);
    ydb_driver_free(drv);
    ydb_query_retry_settings_free(rs, rd);
    return 2;
  }

  int32_t key = 52;
  const char *value = "bebebe bububu";

  st = ydb_params_set_uint64(params, "$key", key, rd);
  check_status(st, "params_set_uint64 $key", rd);

  st = ydb_params_set_utf8(params, "$value", value, rd);
  check_status(st, "params_set_utf8 $value", rd);

  st = ydb_query_begin_tx(qc, YDB_TX_SERIALIZABLE_RW, &tx, rd);
  check_status(st, "begin_tx (UPSERT)", rd);

  st = ydb_query_tx_execute(
      tx, "DECLARE $key AS Uint64;\n"
      "DECLARE $value as Utf8;\n"
      "UPSERT INTO users (key, value) VALUES ($key, $value)", params, NULL, rd);
  check_status(st, "execute UPSERT", rd);

  st = ydb_query_tx_commit(tx, rd);
  check_status(st, "commit UPSERT", rd);
  ydb_query_tx_free(tx, rd);
  tx = NULL;

  ydb_query_params_free(params, rd);
  params = NULL;

  printf("UPSERT succeeded: key=%d, value=%s\n", key, value);

  ydb_query_client_free(qc);
  ydb_driver_free(drv);
  ydb_query_retry_settings_free(rs, rd);
  ydb_result_details_free(rd);

  return 0;
}
