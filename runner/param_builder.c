#include "include/ydb.h"
#include <stdio.h>
#include <stdlib.h>

static const char *details_message(const YdbResultDetails *rd) {
  if (!rd) {
    return "no details";
  }
  return get_message(rd);
}

static void reset_details(YdbResultDetails *rd) {
  if (rd) {
    ydb_result_details_reset(rd);
  }
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

  if (ydb_result_details_init(rd) != 0) {
    fprintf(stderr, "result details creation failed\n");
    return 1;
  }

  cfg = ydb_driver_config_create(rd);
  if (!cfg) {
    fprintf(stderr, "ydb_driver_config_create failure\n");
    ydb_result_details_free(rd);
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
    ydb_result_details_free(rd);
    return 1;
  }

  qc = ydb_query_client_create(drv, rd);
  if (!qc) {
    fprintf(stderr, "ydb_query_client_create failed: %s\n",
            details_message(rd));
    ydb_driver_free(drv);
    ydb_result_details_free(rd);
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
  printf("Table 'users' created (or already exists).\n");

  params = ydb_query_params_create(rd);
  if (!params) {
    fprintf(stderr, "ydb_query_params_create failed: %s\n",
            details_message(rd));
    ydb_query_client_free(qc);
    ydb_driver_free(drv);
    ydb_result_details_free(rd);
    return 2;
  }

  int32_t key = 52;
  const char *value = "bebebe bububu";
  const char raw_payload[] = {'b', 'i', 'n', 'a', 'r', 'y', '\0', 'x'};

  st = ydb_params_set_uint64(params, "$key", key, rd);
  check_status(st, "params_set_uint64 $key", rd);

  st = ydb_params_set_utf8(params, "$value", value, rd);
  check_status(st, "params_set_utf8 $value", rd);

  st = ydb_params_set_int64(params, "$i64", -42, rd);
  check_status(st, "params_set_int64 $i64", rd);

  st = ydb_params_set_double(params, "$d", 3.14159, rd);
  check_status(st, "params_set_double $d", rd);

  st = ydb_params_set_bool(params, "$flag", 1, rd);
  check_status(st, "params_set_bool $flag", rd);

  st = ydb_params_set_bytes(params, "$raw", raw_payload, sizeof(raw_payload), rd);
  check_status(st, "params_set_bytes $raw", rd);

  YdbParamBuilder *pb = ydb_params_begin_param(params, "$list_i32", rd);
  if (!pb) {
    fprintf(stderr, "begin_param $list_i32 failed: %s\n", details_message(rd));
    return 2;
  }
  st = ydb_params_begin_list(pb, rd);
  check_status(st, "begin_list $list_i32", rd);
  st = ydb_params_add_list_item_int32(pb, 10, rd);
  check_status(st, "add_list_item_int32 #1", rd);
  st = ydb_params_add_list_item_int32(pb, 20, rd);
  check_status(st, "add_list_item_int32 #2", rd);
  st = ydb_params_add_list_item_int32(pb, 30, rd);
  check_status(st, "add_list_item_int32 #3", rd);
  st = ydb_params_end_list(pb, rd);
  check_status(st, "end_list $list_i32", rd);
  st = ydb_params_end_param(pb, rd);
  check_status(st, "end_param $list_i32", rd);

  pb = ydb_params_begin_param(params, "$list_utf8", rd);
  if (!pb) {
    fprintf(stderr, "begin_param $list_utf8 failed: %s\n", details_message(rd));
    return 2;
  }
  st = ydb_params_begin_list(pb, rd);
  check_status(st, "begin_list $list_utf8", rd);
  st = ydb_params_add_list_item_utf8(pb, "alpha", rd);
  check_status(st, "add_list_item_utf8 #1", rd);
  st = ydb_params_add_list_item_utf8(pb, "beta", rd);
  check_status(st, "add_list_item_utf8 #2", rd);
  st = ydb_params_end_list(pb, rd);
  check_status(st, "end_list $list_utf8", rd);
  st = ydb_params_end_param(pb, rd);
  check_status(st, "end_param $list_utf8", rd);

  pb = ydb_params_begin_param(params, "$list_opt_i64", rd);
  if (!pb) {
    fprintf(stderr, "begin_param $list_opt_i64 failed: %s\n", details_message(rd));
    return 2;
  }
  st = ydb_params_begin_list(pb, rd);
  check_status(st, "begin_list $list_opt_i64", rd);
  st = ydb_params_add_list_item_null(pb, rd);
  check_status(st, "add_list_item_null #1", rd);
  st = ydb_params_add_list_item_null(pb, rd);
  check_status(st, "add_list_item_null #2", rd);
  st = ydb_params_add_list_item_null(pb, rd);
  check_status(st, "add_list_item_null #3", rd);
  st = ydb_params_end_list(pb, rd);
  check_status(st, "end_list $list_opt_i64", rd);
  st = ydb_params_end_param(pb, rd);
  check_status(st, "end_param $list_opt_i64", rd);

  pb = ydb_params_begin_param(params, "$profile", rd);
  if (!pb) {
    fprintf(stderr, "begin_param $profile failed: %s\n", details_message(rd));
    return 2;
  }
  st = ydb_params_begin_struct(pb, rd);
  check_status(st, "begin_struct $profile", rd);
  st = ydb_params_add_member_bool(pb, "active", 1, rd);
  check_status(st, "add_member_bool active", rd);
  st = ydb_params_add_member_int32(pb, "age", 28, rd);
  check_status(st, "add_member_int32 age", rd);
  st = ydb_params_add_member_double(pb, "score", 9.25, rd);
  check_status(st, "add_member_double score", rd);
  st = ydb_params_add_member_utf8(pb, "nick", "demo", rd);
  check_status(st, "add_member_utf8 nick", rd);
  st = ydb_params_add_member_bytes(pb, "payload", raw_payload, sizeof(raw_payload), rd);
  check_status(st, "add_member_bytes payload", rd);
  st = ydb_params_add_member_null(pb, "note", rd);
  check_status(st, "add_member_null note", rd);
  st = ydb_params_end_struct(pb, rd);
  check_status(st, "end_struct $profile", rd);
  st = ydb_params_end_param(pb, rd);
  check_status(st, "end_param $profile", rd);

  st = ydb_query_begin_tx(qc, YDB_TX_SERIALIZABLE_RW, &tx, rd);
  check_status(st, "begin_tx (UPSERT)", rd);

  st = ydb_query_tx_execute(
      tx,
      "DECLARE $key AS Uint64;\n"
      "DECLARE $value AS Utf8;\n"
      "DECLARE $i64 AS Int64;\n"
      "DECLARE $d AS Double;\n"
      "DECLARE $flag AS Bool;\n"
      "DECLARE $raw AS String;\n"
      "DECLARE $list_i32 AS List<Int32>;\n"
      "DECLARE $list_utf8 AS List<Utf8>;\n"
      "DECLARE $list_opt_i64 AS List<Optional<Int64>>;\n"
      "DECLARE $profile AS Struct<"
      "active:Bool,"
      "age:Int32,"
      "score:Double,"
      "nick:Utf8,"
      "payload:String,"
      "note:Optional<Utf8>>;\n"
      "SELECT "
      "$i64 AS i64, "
      "$d AS d, "
      "$flag AS flag, "
      "$raw AS raw, "
      "ListLength($list_i32) AS list_i32_len, "
      "ListLength($list_utf8) AS list_utf8_len, "
      "ListLength($list_opt_i64) AS list_opt_i64_len, "
      "$profile.active AS profile_active, "
      "$profile.age AS profile_age, "
      "$profile.score AS profile_score, "
      "$profile.nick AS profile_nick;\n"
      "UPSERT INTO users (key, value) VALUES ($key, $value);",
      params, NULL, rd);
  check_status(st, "execute query with mixed params + UPSERT", rd);

  st = ydb_query_tx_commit(tx, rd);
  check_status(st, "commit UPSERT", rd);
  ydb_query_tx_free(tx, rd);
  tx = NULL;

  ydb_query_params_free(params, rd);
  params = NULL;

  printf("UPSERT succeeded: key=%d, value=%s\n", key, value);

  ydb_query_client_free(qc);
  ydb_driver_free(drv);
  ydb_result_details_free(rd);

  return 0;
}
