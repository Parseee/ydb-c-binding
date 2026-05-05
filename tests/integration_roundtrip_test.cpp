#include <gtest/gtest.h>

#include "ydb.h"
#include "ydb_error.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <string>

namespace {

std::string Details(const YdbResultDetails *rd) {
  const char *msg = get_message(rd);
  return msg ? std::string(msg) : std::string("no details");
}

#define AssertOk(st, rd, op)                                                   \
  ASSERT_EQ(st, YDB_OK) << op << " failed: " << Details(rd);

std::string ToHex(const void *data, size_t len) {
  const auto *p = static_cast<const unsigned char *>(data);
  std::ostringstream oss;
  oss << std::hex << std::setfill('0');
  for (size_t i = 0; i < len; ++i) {
    oss << std::setw(2) << static_cast<unsigned int>(p[i]);
  }
  return oss.str();
}

std::string DoubleToString(double v) {
  std::ostringstream oss;
  oss << std::setprecision(17) << v;
  return oss.str();
}

const char *EnvOrDefault(const char *key, const char *fallback) {
  const char *value = std::getenv(key);
  return (value && value[0] != '\0') ? value : fallback;
}

} // namespace

TEST(YdbIntegration, RoundTripScalarTypesAsStrings) {
  YdbResultDetails *rd = nullptr;
  ASSERT_EQ(ydb_result_details_init(&rd), 0);
  ASSERT_NE(rd, nullptr);

  YdbDriverConfig *cfg = ydb_driver_config_create(rd);
  ASSERT_NE(cfg, nullptr) << Details(rd);

  AssertOk(ydb_driver_config_set_endpoint(cfg, "ydb-local:2136", rd), rd,
           "set_endpoint");
  AssertOk(ydb_driver_config_set_database(cfg, "/local", rd), rd,
           "set_database");

  YdbDriver *drv = ydb_driver_create(cfg, rd);
  ydb_driver_config_free(cfg);
  cfg = nullptr;
  if (!drv) {
    GTEST_SKIP() << "driver creation failed, skipping integration test: "
                 << Details(rd);
  }

  YdbQueryClient *qc = ydb_query_client_create(drv, rd);
  if (!qc) {
    ydb_driver_free(drv);
    GTEST_SKIP() << "query client creation failed, skipping integration test: "
                 << Details(rd);
  }

  AssertOk(
      ydb_query_NOtx_execute(qc,
                             "CREATE TABLE IF NOT EXISTS c_binding_roundtrip ("
                             "  id Uint64,"
                             "  c_u64 Uint64,"
                             "  c_i64 Int64,"
                             "  c_dbl Double,"
                             "  c_bool Bool,"
                             "  c_utf8 Utf8,"
                             "  c_raw String,"
                             "  c_opt_utf8 Utf8?,"
                             "  PRIMARY KEY(id)"
                             ");",
                             nullptr, nullptr, rd),
      rd, "create table");

  YdbQueryParams *params = ydb_query_params_create(rd);
  ASSERT_NE(params, nullptr) << Details(rd);

  const uint64_t id = 9000001ULL;
  const uint64_t expected_u64 = 1844674407370955161ULL;
  const int64_t expected_i64 = -922337203685477500LL;
  const double expected_dbl = 3.141592653589793;
  const int expected_bool = 1;
  const char *expected_utf8 = "hello-ydb";
  const char expected_raw[] = {'A', '\0', 'B', '\x7f', 'Z'};

  AssertOk(ydb_params_set_uint64(params, "$id", id, rd), rd, "set $id");
  AssertOk(ydb_params_set_uint64(params, "$u64", expected_u64, rd), rd,
           "set $u64");
  AssertOk(ydb_params_set_int64(params, "$i64", expected_i64, rd), rd,
           "set $i64");
  AssertOk(ydb_params_set_double(params, "$dbl", expected_dbl, rd), rd,
           "set $dbl");
  AssertOk(ydb_params_set_bool(params, "$b", expected_bool, rd), rd, "set $b");
  AssertOk(ydb_params_set_utf8(params, "$u", expected_utf8, rd), rd, "set $u");
  AssertOk(ydb_params_set_bytes(params, "$raw", expected_raw,
                                sizeof(expected_raw), rd),
           rd, "set $raw");

  YdbQueryTransaction *tx = nullptr;
  AssertOk(ydb_query_begin_tx(qc, YDB_TX_SERIALIZABLE_RW, &tx, rd), rd,
           "begin tx");

  AssertOk(
      ydb_query_tx_execute(
          tx,
          "DECLARE $id AS Uint64;\n"
          "DECLARE $u64 AS Uint64;\n"
          "DECLARE $i64 AS Int64;\n"
          "DECLARE $dbl AS Double;\n"
          "DECLARE $b AS Bool;\n"
          "DECLARE $u AS Utf8;\n"
          "DECLARE $raw AS String;\n"
          "UPSERT INTO c_binding_roundtrip "
          "(id, c_u64, c_i64, c_dbl, c_bool, c_utf8, c_raw, c_opt_utf8) "
          "VALUES ($id, $u64, $i64, $dbl, $b, $u, $raw, CAST(NULL AS Utf8?));",
          params, nullptr, rd),
      rd, "upsert");

  AssertOk(ydb_query_tx_commit(tx, rd), rd, "commit");
  ydb_query_tx_free(tx, rd);
  tx = nullptr;
  ydb_query_params_free(params, rd);
  params = nullptr;

  YdbResultSets *all_results = nullptr;
  AssertOk(ydb_query_NOtx_execute(
               qc,
               "SELECT c_u64, c_i64, c_dbl, c_bool, c_utf8, c_raw, c_opt_utf8 "
               "FROM c_binding_roundtrip WHERE id = 9000001u;",
               nullptr, &all_results, rd),
           rd, "select");
  ASSERT_NE(all_results, nullptr);

  YdbResultSet *rs = ydb_resultsets_release(all_results, 0, rd);
  ASSERT_NE(rs, nullptr) << Details(rd);
  ydb_resultsets_free(all_results, rd);
  all_results = nullptr;

  ASSERT_EQ(ydb_resultset_next_row(rs, rd), 1) << Details(rd);

  uint64_t got_u64 = 0;
  int64_t got_i64 = 0;
  double got_dbl = 0.0;
  int got_bool = 0;
  const char *got_utf8 = nullptr;
  size_t got_utf8_len = 0;
  const void *got_raw = nullptr;
  size_t got_raw_len = 0;

  AssertOk(ydb_resultset_get_uint64(rs, 0, &got_u64, rd), rd, "get c_u64");
  AssertOk(ydb_resultset_get_int64(rs, 1, &got_i64, rd), rd, "get c_i64");
  AssertOk(ydb_resultset_get_double(rs, 2, &got_dbl, rd), rd, "get c_dbl");
  AssertOk(ydb_resultset_get_bool(rs, 3, &got_bool, rd), rd, "get c_bool");
  AssertOk(ydb_resultset_get_utf8_view(rs, 4, &got_utf8, &got_utf8_len, rd), rd,
           "get c_utf8");
  std::string recv_utf8_str(got_utf8, got_utf8_len);
  AssertOk(ydb_resultset_get_bytes_view(rs, 5, &got_raw, &got_raw_len, rd), rd,
           "get c_raw");
  std::string recv_raw_str((char *)got_raw, got_raw_len);

  EXPECT_EQ(std::to_string(got_u64), std::to_string(expected_u64));
  EXPECT_EQ(std::to_string(got_i64), std::to_string(expected_i64));
  EXPECT_EQ(DoubleToString(got_dbl), DoubleToString(expected_dbl));
  EXPECT_EQ(std::string(got_bool ? "true" : "false"),
            std::string(expected_bool ? "true" : "false"));
  EXPECT_EQ(recv_utf8_str, std::string(expected_utf8));
  EXPECT_EQ(ToHex(recv_raw_str.c_str(), got_raw_len),
            ToHex(expected_raw, sizeof(expected_raw)));

  EXPECT_EQ(ydb_resultset_column_type(rs, 6, rd), YDB_TYPE_OPTIONAL)
      << "type mismatch (expected optional)\n";
  EXPECT_EQ(ydb_resultset_next_row(rs, rd), 0);

  ydb_query_client_free(qc);
  ydb_driver_free(drv);
  ydb_result_details_free(rd);
}
