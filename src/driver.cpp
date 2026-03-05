#include "include/ydb.h"

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <memory>
#include <string>

struct YdbDriverConfig {
  std::string endpoint;
  std::string database;
  std::string auth_token;
};

struct YdbDriver {
  std::unique_ptr<NYdb::TDriverConfig> config;
  std::unique_ptr<NYdb::TDriver> driver;
};

struct YdbTableClient {
  std::unique_ptr<NYdb::NTable::TTableClient> client;
  YdbDriver *parent_driver;
};

struct YdbQueryParams {
  NYdb::TParamsBuilder builder;
};

struct YdbResultSets {};
struct YdbResultSet {};
struct YdbTransaction {};

static thread_local std::string g_last_error;
static void set_last_error(const std::string &msg) { g_last_error = msg; }

extern "C" {

int ydb_version_major(void) { return YDB_C_API_VERSION_MAJOR; }
int ydb_version_minor(void) { return YDB_C_API_VERSION_MINOR; }
int ydb_version_patch(void) { return YDB_C_API_VERSION_PATCH; }

const char *ydb_last_error_message(void) { return g_last_error.c_str(); }

YdbDriverConfig *ydb_driver_config_create(void) {
  return new (std::nothrow) YdbDriverConfig();
}
void ydb_driver_config_free(YdbDriverConfig *cfg) { delete cfg; }

ydb_status_t ydb_driver_config_set_endpoint(YdbDriverConfig *cfg,
                                            const char *v) {
  if (!cfg || !v) {
    return YDB_ERR_BAD_REQUEST;
  }
  cfg->endpoint = v;
  return YDB_OK;
}
ydb_status_t ydb_driver_config_set_database(YdbDriverConfig *cfg,
                                            const char *v) {
  if (!cfg || !v) {
    return YDB_ERR_BAD_REQUEST;
  }
  cfg->database = v;
  return YDB_OK;
}
ydb_status_t ydb_driver_config_set_auth_token(YdbDriverConfig *cfg,
                                              const char *v) {
  if (!cfg || !v)
    return YDB_ERR_BAD_REQUEST;
  cfg->auth_token = v;
  return YDB_OK;
}

YdbDriver *ydb_driver_create(const YdbDriverConfig *cfg) {
  if (!cfg) {
    set_last_error("config is null");
    return nullptr;
  }

  auto *drv = new (std::nothrow) YdbDriver();
  if (!drv)
    return nullptr;

  try {
    auto c = NYdb::TDriverConfig()
                 .SetEndpoint(cfg->endpoint)
                 .SetDatabase(cfg->database)
                 .SetAuthToken(cfg->auth_token);
    drv->config = std::make_unique<NYdb::TDriverConfig>(std::move(c));
    drv->driver = std::make_unique<NYdb::TDriver>(*drv->config);
  } catch (const std::exception &e) {
    set_last_error(e.what());
    delete drv;
    return nullptr;
  }
  return drv;
}

ydb_status_t ydb_driver_start(YdbDriver *drv) {
  return drv ? YDB_OK : YDB_ERR_BAD_REQUEST;
}
ydb_status_t ydb_driver_wait_ready(YdbDriver *drv, int) {
  return drv ? YDB_OK : YDB_ERR_BAD_REQUEST;
}
void ydb_driver_free(YdbDriver *drv) {
  if (!drv)
    return;
  if (drv->driver)
    drv->driver->Stop(true);
  delete drv;
}

YdbTableClient *ydb_table_client_create(YdbDriver *drv) {
  if (!drv || !drv->driver) {
    set_last_error("driver is null");
    return nullptr;
  }

  auto *tc = new (std::nothrow) YdbTableClient();
  if (!tc)
    return nullptr;

  try {
    tc->client = std::make_unique<NYdb::NTable::TTableClient>(*drv->driver);
    tc->parent_driver = drv;
  } catch (const std::exception &e) {
    set_last_error(e.what());
    delete tc;
    return nullptr;
  }
  return tc;
}
void ydb_table_client_free(YdbTableClient *tc) { delete tc; }

YdbQueryParams *ydb_query_params_create(void) {
  return new (std::nothrow) YdbQueryParams();
}
void ydb_query_params_free(YdbQueryParams *p) { delete p; }

ydb_status_t ydb_query_params_set_utf8(YdbQueryParams *p, const char *name,
                                       const char *value) {
  if (!p || !name || !value)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name).Utf8(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_query_params_set_int64(YdbQueryParams *p, const char *name,
                                        int64_t value) {
  if (!p || !name)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name).Int64(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_query_params_set_uint64(YdbQueryParams *p, const char *name,
                                         uint64_t value) {
  if (!p || !name)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name).Uint64(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_query_params_set_double(YdbQueryParams *p, const char *name,
                                         double value) {
  if (!p || !name)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name).Double(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_query_params_set_bool(YdbQueryParams *p, const char *name,
                                       int value) {
  if (!p || !name)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name).Bool(value != 0).Build();
  return YDB_OK;
}
ydb_status_t ydb_query_params_set_bytes(YdbQueryParams *p, const char *name,
                                        const void *data, size_t len) {
  if (!p || !name || !data)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name)
      .String(std::string(static_cast<const char *>(data), len))
      .Build();
  return YDB_OK;
}

ydb_status_t ydb_table_execute_scheme(YdbTableClient *tc, const char *yql) {
  if (!tc || !yql)
    return YDB_ERR_BAD_REQUEST;
  try {
    auto st =
        tc->client->RetryOperationSync([&](NYdb::NTable::TSession session) {
          return session.ExecuteSchemeQuery(yql).GetValueSync();
        });
    if (!st.IsSuccess()) {
      set_last_error(st.GetIssues().ToString());
      return YDB_ERR_GENERIC;
    }
  } catch (const std::exception &e) {
    set_last_error(e.what());
    return YDB_ERR_INTERNAL;
  }
  return YDB_OK;
}

ydb_status_t ydb_table_execute_query(YdbTableClient *tc, const char *yql,
                                     const YdbQueryParams *params,
                                     YdbResultSets **out_results) {
  if (!tc || !yql)
    return YDB_ERR_BAD_REQUEST;
  try {
    auto st = tc->client->RetryOperationSync(
        [&](NYdb::NTable::TSession session) -> NYdb::TStatus {
          auto tx = NYdb::NTable::TTxControl::BeginTx(
                        NYdb::NTable::TTxSettings::SerializableRW())
                        .CommitTx();

          if (params) {
            return session
                .ExecuteDataQuery(
                    yql, tx,
                    const_cast<YdbQueryParams *>(params)->builder.Build())
                .GetValueSync();
          }
          return session.ExecuteDataQuery(yql, tx).GetValueSync();
        });
    if (!st.IsSuccess()) {
      set_last_error(st.GetIssues().ToString());
      return YDB_ERR_GENERIC;
    }
  } catch (const std::exception &e) {
    set_last_error(e.what());
    return YDB_ERR_INTERNAL;
  }

  if (out_results)
    *out_results = nullptr; // TODO: OUT is missing
  return YDB_OK;
}

ydb_status_t ydb_table_begin_tx(YdbTableClient *, int, YdbTransaction **) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_tx_execute(YdbTransaction *, const char *,
                            const YdbQueryParams *, YdbResultSets **) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_tx_commit(YdbTransaction *) { return YDB_ERR_GENERIC; }
ydb_status_t ydb_tx_rollback(YdbTransaction *) { return YDB_ERR_GENERIC; }
void ydb_tx_free(YdbTransaction *) {}

int ydb_result_sets_count(const YdbResultSets *) { return 0; }
YdbResultSet *ydb_result_sets_get(YdbResultSets *, int) { return nullptr; }
void ydb_result_sets_free(YdbResultSets *rs) { delete rs; }

int ydb_result_set_column_count(const YdbResultSet *) { return 0; }
const char *ydb_result_set_column_name(const YdbResultSet *, int) {
  return nullptr;
}
int ydb_result_set_column_type(const YdbResultSet *, int) { return 0; }
int ydb_result_set_next_row(YdbResultSet *) { return 0; }
int ydb_result_set_is_null(YdbResultSet *, int) { return 1; }

ydb_status_t ydb_result_set_get_utf8(YdbResultSet *, int, const char **,
                                     size_t *) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_result_set_get_int64(YdbResultSet *, int, int64_t *) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_result_set_get_uint64(YdbResultSet *, int, uint64_t *) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_result_set_get_double(YdbResultSet *, int, double *) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_result_set_get_bool(YdbResultSet *, int, int *) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_result_set_get_bytes(YdbResultSet *, int, const void **,
                                      size_t *) {
  return YDB_ERR_GENERIC;
}

} // extern "C"