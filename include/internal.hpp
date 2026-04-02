#pragma once

#include "ydb.h"
#include "ydb_error.h"

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/result/result.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <memory>
#include <string>
#include <vector>

#define RD(code, details) ydb_result_details_fail(rd, code, details)
#define CHECK_RD(rd)                                                           \
  do {                                                                         \
    if (rd == NULL) {                                                          \
      return YDB_OK;                                                           \
    }                                                                          \
    if (isFatal(rd)) {                                                         \
      std::string error_msg = std::string("from ") + __func__;                 \
      ydb_result_details_append_message(rd, error_msg.c_str());                \
      return rd->code;                                                         \
    }                                                                          \
  } while (0)

struct YdbDriverConfig {
  std::string endpoint;
  std::string database;
  std::string auth_token;
};

struct YdbDriver {
  std::unique_ptr<NYdb::TDriverConfig> config;
  std::unique_ptr<NYdb::TDriver> driver;
};

struct YdbQueryParams {
  NYdb::TParamsBuilder builder;
};

struct YdbParamBuilder {
  YdbQueryParams *owner;
  NYdb::TParamValueBuilder *slot;
};

/* ── Results ─────────────────────────────────────────────────────── */

struct YdbResultSet {
  NYdb::TResultSet resultSet;
  NYdb::TResultSetParser parser;

  explicit YdbResultSet(NYdb::TResultSet rs)
      : resultSet(std::move(rs)), parser(resultSet) {}
};

struct YdbResultSets {
  std::vector<YdbResultSet *> sets;
  ~YdbResultSets() {
    for (auto *s : sets)
      delete s;
  }
};

/* ── Query Service ───────────────────────────────────────────────── */

struct YdbQueryClient {
  std::unique_ptr<NYdb::NQuery::TQueryClient> client;
  YdbDriver *parent_driver;
};

struct YdbQueryTransaction {
  NYdb::NQuery::TTransaction tx;
  NYdb::NQuery::TSession session;
  YdbQueryTransaction(NYdb::NQuery::TSession s, NYdb::NQuery::TTransaction t)
      : tx(std::move(t)), session(std::move(s)) {}
};

struct YdbQueryRetrySettings {
  uint32_t max_retries;
  uint32_t current_retries;
  uint32_t timeout_ms;
};

ydb_status_t status_to_ydb_code(NYdb::EStatus s);
ydb_status_t ydb_fill_from_status(YdbResultDetails *details,
                                  const NYdb::TStatus &st);

bool isFatal(YdbResultDetails *rd);
