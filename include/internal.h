#pragma once

#include "include/ydb.h"

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/result/result.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <memory>
#include <string>
#include <vector>

typedef enum ydb_type_t : uint32_t {
  YDB_TYPE_BOOL = 0x0006,
  YDB_TYPE_INT8 = 0x0007,
  YDB_TYPE_UINT8 = 0x0005,
  YDB_TYPE_INT16 = 0x0008,
  YDB_TYPE_UINT16 = 0x0009,
  YDB_TYPE_INT32 = 0x0001,
  YDB_TYPE_UINT32 = 0x0002,
  YDB_TYPE_INT64 = 0x0003,
  YDB_TYPE_UINT64 = 0x0004,
  YDB_TYPE_FLOAT = 0x0021,
  YDB_TYPE_DOUBLE = 0x0020,
  YDB_TYPE_DATE = 0x0030,
  YDB_TYPE_DATETIME = 0x0031,
  YDB_TYPE_TIMESTAMP = 0x0032,
  YDB_TYPE_INTERVAL = 0x0033,
  YDB_TYPE_BYTES = 0x1001,
  YDB_TYPE_UTF8 = 0x1200,
  YDB_TYPE_JSON = 0x1202,
  YDB_TYPE_UUID = 0x1203,
  YDB_TYPE_JSON_DOC = 0x1204,
  YDB_TYPE_OPTIONAL = 0x0100,
  YDB_TYPE_UNKNOWN = 0x0000,
} ydb_type_t;

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

/* ── Thread-local error ──────────────────────────────────────────── */

extern thread_local std::string g_last_error;
void set_last_error(const std::string &msg);
ydb_status_t status_to_ydb_code(NYdb::EStatus s);