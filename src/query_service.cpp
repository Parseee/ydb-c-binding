#include "include/internal.h"

#include <optional>
#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/query/fwd.h>
#include <ydb-cpp-sdk/client/query/query.h>
#include <ydb-cpp-sdk/client/query/tx.h>

#include <memory>
#include <string>

extern "C" {

YdbQueryClient *ydb_query_client_create(YdbDriver *drv) {
  if (!drv || !drv->driver) {
    set_last_error("driver is null");
    return nullptr;
  }

  auto *qc = new (std::nothrow) YdbQueryClient();
  if (!qc) {
    set_last_error("failed to allocate query client");
    return nullptr;
  }

  try {
    qc->client = std::make_unique<NYdb::NQuery::TQueryClient>(*drv->driver);
    qc->parent_driver = drv;
  } catch (const std::exception &e) {
    set_last_error(e.what());
    delete qc;
    return nullptr;
  }
  return qc;
}
void ydb_query_client_free(YdbQueryClient *qc) { delete qc; }

ydb_status_t ydb_query_execute(YdbQueryClient *qc, const char *yql,
                               ydb_tx_mode_t tx_mode,
                               const YdbQueryParams *params,
                               YdbResultSets **out_results) {
  if (!qc || !yql) {
    return YDB_ERR_BAD_REQUEST;
  }

  // Build params ONCE here, outside the retry lambda.
  // TParamsBuilder::Build() is destructive — calling it inside a retried
  // lambda would produce empty params on the second attempt.
  std::optional<NYdb::TParams> sdk_params;
  if (params) {
    sdk_params = const_cast<YdbQueryParams *>(params)->builder.Build();
  }

  auto tx_control = [&]() -> NYdb::NQuery::TTxControl {
    switch (tx_mode) {
    case YDB_TX_SERIALIZABLE_RW:
      return NYdb::NQuery::TTxControl::BeginTx(
                 NYdb::NQuery::TTxSettings::SerializableRW())
          .CommitTx();
    case YDB_TX_SNAPSHOT_RO:
      return NYdb::NQuery::TTxControl::BeginTx(
                 NYdb::NQuery::TTxSettings::SnapshotRO())
          .CommitTx();
    case YDB_TX_STALE_RO:
      return NYdb::NQuery::TTxControl::BeginTx(
                 NYdb::NQuery::TTxSettings::StaleRO())
          .CommitTx();
    case YDB_TX_ONLINE_RO:
      return NYdb::NQuery::TTxControl::BeginTx(
                 NYdb::NQuery::TTxSettings::OnlineRO())
          .CommitTx();
    case YDB_TX_SNAPSHOT_RW:
      return NYdb::NQuery::TTxControl::BeginTx(
                 NYdb::NQuery::TTxSettings::SnapshotRW())
          .CommitTx();
    case YDB_TX_NONE:
    default:
      return NYdb::NQuery::TTxControl::NoTx();
    }
  }();

  YdbResultSets *rs_out = nullptr;
  try {
    NYdb::TStatus status = qc->client->RetryQuerySync(
        [&](NYdb::NQuery::TSession session) -> NYdb::TStatus {
          // On retry: discard any partial results from the previous attempt.
          delete rs_out;
          rs_out = nullptr;

          auto result =
              sdk_params.has_value()
                  ? session.ExecuteQuery(yql, tx_control, *sdk_params)
                        .ExtractValueSync()
                  : session.ExecuteQuery(yql, tx_control).ExtractValueSync();

          if (!result.IsSuccess()) {
            set_last_error(result.GetIssues().ToString());
            return result;
          }

          // Collect all result sets from the successful execution.
          auto *rs = new (std::nothrow) YdbResultSets();
          if (!rs) {
            set_last_error("failed to allocate result sets");
            return NYdb::TStatus(NYdb::EStatus::CLIENT_OUT_OF_RANGE,
                                 NYdb::NIssue::TIssues{});
          }

          for (auto &rset : result.GetResultSets()) {
            auto *set = new (std::nothrow) YdbResultSet(std::move(rset));
            if (!set) {
              delete rs;
              set_last_error("failed to allocate a result set");
              return NYdb::TStatus(NYdb::EStatus::CLIENT_OUT_OF_RANGE,
                                   NYdb::NIssue::TIssues{});
            }
          }

          rs_out = rs;
          return result;
        });

    if (!status.IsSuccess()) {
      delete rs_out;
      if (g_last_error.empty()) {
        set_last_error(status.GetIssues().ToString());
      }
      return YDB_ERR_GENERIC;
    }
  } catch (const std::exception &e) {
    delete rs_out;
    set_last_error(e.what());
    return YDB_ERR_INTERNAL;
  }

  if (out_results) {
    *out_results = rs_out;
  } else {
    delete rs_out;
  }

  return YDB_OK;
}

ydb_status_t ydb_tx_execute(YdbQueryTransaction *, const char *,
                            const YdbQueryParams *, YdbResultSets **) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_tx_commit(YdbQueryTransaction *) { return YDB_ERR_GENERIC; }
ydb_status_t ydb_tx_rollback(YdbQueryTransaction *) { return YDB_ERR_GENERIC; }
void ydb_tx_free(YdbQueryTransaction *) {}

} // extern "C"