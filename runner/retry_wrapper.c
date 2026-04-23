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
  YdbQueryParams *params = NULL;
  YdbQueryRetrySettings *rs = NULL;
  YdbQueryTransaction *tx = NULL;

  cfg = ydb_driver_config_create(rd);
  if (!cfg) {
    fprintf(stderr, "ydb_driver_config_create failed\n");
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
    fprintf(stderr, "ydb_query_client_create failed: %s\n", details_message(rd));
    ydb_driver_free(drv);
    return 1;
  }

  /* DDL in NO-TX mode */
  st = ydb_query_NOtx_execute(qc,
                              "CREATE TABLE users ("
                              "id Uint64,"
                              "name Utf8,"
                              "PRIMARY KEY (id));",
                              NULL, NULL, rd);
  check_status(st, "create users table", rd);

  params = ydb_query_params_create(rd);
  if (!params) {
    fprintf(stderr, "ydb_query_params_create failed: %s\n", details_message(rd));
    ydb_query_client_free(qc);
    ydb_driver_free(drv);
    return 1;
  }

  st = ydb_params_set_uint64(params, "$id", 42, rd);
  check_status(st, "set $id", rd);
  st = ydb_params_set_utf8(params, "$name", "manual-retry-demo", rd);
  check_status(st, "set $name", rd);

  /* User-managed retry policy with library-provided settings object. */
  rs = ydb_query_retry_settings_create(5, 200, rd);
  if (!rs) {
    fprintf(stderr, "ydb_query_retry_settings_create failed: %s\n",
            details_message(rd));
    ydb_query_params_free(params, rd);
    ydb_query_client_free(qc);
    ydb_driver_free(drv);
    return 1;
  }

  for (;;) {
    st = ydb_query_begin_tx(qc, YDB_TX_SERIALIZABLE_RW, &tx, rd);
    if (st != YDB_OK) {
      if (!ydb_is_status_retriable(st) ||
          ydb_query_perform_retry(rs, rd) != YDB_OK) {
        check_status(st, "begin_tx", rd);
      }
      continue;
    }

    st = ydb_query_tx_execute(
        tx, "UPSERT INTO users (id, name) VALUES ($id, $name)", params, NULL, rd);
    if (st == YDB_OK) {
      st = ydb_query_tx_commit(tx, rd);
    } else {
      (void)ydb_query_tx_rollback(tx, rd);
    }
    ydb_query_tx_free(tx, rd);
    tx = NULL;

    if (st == YDB_OK) {
      break;
    }
    if (!ydb_is_status_retriable(st) || ydb_query_perform_retry(rs, rd) != YDB_OK) {
      check_status(st, "upsert with retries", rd);
    }
  }

  printf("UPSERT succeeded with user-managed retry loop.\n");

  ydb_query_retry_settings_free(rs, rd);
  ydb_query_params_free(params, rd);
  ydb_query_client_free(qc);
  ydb_driver_free(drv);
  return 0;
}
