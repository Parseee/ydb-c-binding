#pragma once

#include "ydb.h"
#include "ydb_error.h"

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/result/result.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

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
  std::string scratch;

  explicit YdbResultSet(NYdb::TResultSet rs)
      : resultSet(std::move(rs)), parser(resultSet) {}
};

struct YdbResultSets {
  std::vector<std::unique_ptr<YdbResultSet>> sets;
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

/* ── Error Handling ──────────────────────────────────────────────── */

struct YdbResultDetails {
  ydb_status_t code;
  std::string message;
  std::string context;
};

ydb_status_t status_to_ydb_code(NYdb::EStatus s);
ydb_status_t ydb_fill_from_status(YdbResultDetails *details,
                                  const NYdb::TStatus &st);

bool isFatal(YdbResultDetails *rd);

void ydb_result_details_set_status(YdbResultDetails *rd, ydb_status_t code);
void ydb_result_details_set_message(YdbResultDetails *rd,
                                    const std::string &msg);
void ydb_result_details_append_message(YdbResultDetails *rd,
                                       const std::string &msg);
void ydb_result_details_set_context(YdbResultDetails *rd,
                                    const std::string &ctx);

ydb_status_t ydb_rd_fail(YdbResultDetails *rd, ydb_status_t code,
                         const char *details);

void ydb_append_fatal_context(YdbResultDetails *rd, const char *func);

std::optional<ydb_status_t> ydb_check_rd_status(YdbResultDetails *rd,
                                                const char *func);

bool ydb_check_rd_fatal(YdbResultDetails *rd, const char *func);