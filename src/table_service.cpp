#include "include/internal.h"

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <memory>
#include <string>

extern "C" {
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

ydb_status_t ydb_table_begin_tx(YdbTableClient *, ydb_tx_mode_t,
                                YdbTransaction **) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_tx_execute(YdbTransaction *, const char *,
                            const YdbQueryParams *, YdbResultSets **) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_tx_commit(YdbTransaction *) { return YDB_ERR_GENERIC; }
ydb_status_t ydb_tx_rollback(YdbTransaction *) { return YDB_ERR_GENERIC; }
void ydb_tx_free(YdbTransaction *) {}
}