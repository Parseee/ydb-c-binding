#pragma once

#include "ydb.h"
#include "ydb_error.h"

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/result/result.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <cstddef>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <utility>
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
void ydb_result_details_set_message(YdbResultDetails *rd, std::string msg);
void ydb_result_details_append_message(YdbResultDetails *rd, std::string msg);
void ydb_result_details_set_context(YdbResultDetails *rd, std::string ctx);
void ydb_result_details_print(const std::string &msg);

void ydb_append_fatal_context(YdbResultDetails *rd, const char *func);

std::optional<ydb_status_t> ydb_check_rd_status(YdbResultDetails *rd,
                                                const char *func);
ydb_status_t ydb_result_details_fail(YdbResultDetails *rd, ydb_status_t code,
                                     std::string msg);

bool ydb_check_rd_fatal(YdbResultDetails *rd, const char *func);

template <typename Func, typename FailWith> struct YdbExceptionGuard {
  FailWith fail;
  YdbResultDetails *rd;
  Func func;

  explicit YdbExceptionGuard(FailWith fail_with, YdbResultDetails *rd, Func f)
      : fail(std::move(fail_with)), rd(rd), func(std::move(f)) {}

  template <typename... Args>
  auto
  operator()(Args &&...args) -> decltype(func(std::forward<Args>(args)...)) {
    try {
      return std::forward<Func>(func)(std::forward<Args>(args)...);
    } catch (const std::exception &e) {
      return std::forward<FailWith>(fail)(rd, YDB_ERR_INTERNAL, e.what());
    } catch (...) {
      return std::forward<FailWith>(fail)(rd, YDB_ERR_INTERNAL,
                                          "uncaught cpp exception");
    }
  }
};

inline ydb_status_t ydb_fail_status(YdbResultDetails *rd, ydb_status_t code,
                                    const char *msg) {
  return ydb_result_details_fail(rd, code, msg ? msg : "");
}

inline std::nullptr_t ydb_fail_ptr(YdbResultDetails *rd, ydb_status_t code,
                                   const char *msg) {
  (void)ydb_result_details_fail(rd, code, msg ? msg : "");
  return nullptr;
}

inline void ydb_fail_void(YdbResultDetails *rd, ydb_status_t code,
                          const char *msg) {
  (void)ydb_result_details_fail(rd, code, msg ? msg : "");
}

// 0 := error
inline int ydb_fail_int(YdbResultDetails *rd, ydb_status_t code,
                        const char *msg) {
  ydb_result_details_fail(rd, code, msg ? msg : "");
  return 0;
}

inline ydb_type_t ydb_fail_type_t(YdbResultDetails *rd, ydb_status_t code,
                                  const char *msg) {
  ydb_result_details_fail(rd, code, msg ? msg : "");
  return YDB_TYPE_UNKNOWN;
}