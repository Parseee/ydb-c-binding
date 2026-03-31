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

struct YdbDriverConfig {
  std::string endpoint;
  std::string database;
  std::string auth_token;
};

struct YdbDriver {
  std::unique_ptr<NYdb::TDriverConfig> config;
  std::unique_ptr<NYdb::TDriver> driver;
  ydb_result_details_t result_details;
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

/* ── Table Service ───────────────────────────────────────────────── */

struct YdbTableClient {
  std::unique_ptr<NYdb::NTable::TTableClient> client;
  YdbDriver *parent_driver;
};

struct YdbSession {
  NYdb::NTable::TSession session;
};

struct YdbTableTransaction {
  NYdb::NTable::TTransaction tx;
  YdbTableTransaction(NYdb::NTable::TTransaction t) : tx(std::move(t)) {}
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

ydb_status_t status_to_ydb_code(NYdb::EStatus s);
