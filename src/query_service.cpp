#include "include/internal.hpp"
#include "ydb.h"
#include "ydb_error.h"

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/query/query.h>
#include <ydb-cpp-sdk/client/query/tx.h>

#include <memory>
#include <optional>
#include <string>

extern "C" {

YdbQueryClient *ydb_query_client_create(YdbDriver *drv,
                                        ydb_result_details_t *rd) {
  if (!drv || !drv->driver) {
    ydb_result_details_set_message(rd, "driver is null");
    ydb_result_details_set_status(rd, YDB_ERR_INTERNAL);
    return nullptr;
  }

  auto *qc = new (std::nothrow) YdbQueryClient();
  if (!qc) {
    ydb_result_details_set_message(rd, "failed to allocate query client");
    ydb_result_details_set_status(rd, YDB_ERR_INTERNAL);
    return nullptr;
  }

  try {
    qc->client = std::make_unique<NYdb::NQuery::TQueryClient>(*drv->driver);
    qc->parent_driver = drv;
  } catch (const std::exception &e) {
    ydb_result_details_set_message(rd, e.what());
    ydb_result_details_set_status(rd, YDB_ERR_INTERNAL);
    delete qc;
    return nullptr;
  }
  return qc;
}
void ydb_query_client_free(YdbQueryClient *qc) {
  delete qc;
}

ydb_status_t ydb_query_execute(YdbQueryClient *qc, const char *yql,
                               ydb_tx_mode_t tx_mode,
                               const YdbQueryParams *params,
                               YdbResultSets **out_results,
                               ydb_result_details_t *result_details) {
  if (!qc || !yql) {
    return YDB_ERR_BAD_REQUEST;
  }

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

  const bool is_readonly =
      (tx_mode == YDB_TX_SNAPSHOT_RO || tx_mode == YDB_TX_STALE_RO ||
       tx_mode == YDB_TX_ONLINE_RO);

  auto retry_settings =
      NYdb::NRetry::TRetryOperationSettings().MaxRetries(10).Idempotent(
          is_readonly);

  YdbResultSets *rs_out = nullptr;
  try {
    NYdb::TStatus status = qc->client->RetryQuerySync(
        [&](NYdb::NQuery::TSession session) -> NYdb::TStatus {
          // On retry: discard any partial results from the previous attempt.
          delete rs_out;
          rs_out = nullptr;
          // TODO:
          // здесь мы закрыты. нельзя ретраить извне
          // запускаем ретраер
          // случается ошибка
          // предоставляем настройки для ретрая
          // сохраняем данные для принятия решения ретрая
          // функция принимает решение

          auto result =
              sdk_params.has_value()
                  ? session.ExecuteQuery(yql, tx_control, *sdk_params)
                        .ExtractValueSync()
                  : session.ExecuteQuery(yql, tx_control).ExtractValueSync();

          if (!result.IsSuccess()) {
            ydb_result_details_print(result.GetIssues().ToString().c_str());
            return result;
          }

          // Collect all result sets from the successful execution.
          auto *rs = new (std::nothrow) YdbResultSets();
          if (!rs) {
            ydb_result_details_print("failed to allocate result sets");
            return NYdb::TStatus(NYdb::EStatus::CLIENT_OUT_OF_RANGE,
                                 NYdb::NIssue::TIssues{});
          }

          for (auto &rset : result.GetResultSets()) {
            auto *set = new (std::nothrow) YdbResultSet(std::move(rset));
            if (!set) {
              delete rs;
              ydb_result_details_print("failed to allocate a result set");
              return NYdb::TStatus(NYdb::EStatus::CLIENT_OUT_OF_RANGE,
                                   NYdb::NIssue::TIssues{});
            }
          }

          rs_out = rs;
          return result;
        });

    if (!status.IsSuccess()) {
      delete rs_out;
      ydb_result_details_print(status.GetIssues().ToString().c_str());
      return YDB_ERR_INTERNAL;
    }
  } catch (const std::exception &e) {
    delete rs_out;
    ydb_result_details_print(e.what());
    return YDB_ERR_INTERNAL;
  }

  if (out_results) {
    *out_results = rs_out;
  } else {
    delete rs_out;
  }

  return YDB_OK;
}

ydb_status_t ydb_query_tx_execute(YdbQueryTransaction *, const char *,
                                  const YdbQueryParams *, YdbResultSets **,
                                  ydb_result_details_t *result_details) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_query_tx_commit(YdbQueryTransaction *,
                                 ydb_result_details_t *result_details) {
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_query_tx_rollback(YdbQueryTransaction *,
                                   ydb_result_details_t *result_details) {
  return YDB_ERR_GENERIC;
}
void ydb_query_tx_free(YdbQueryTransaction *,
                       ydb_result_details_t *result_details) {}

} // extern "C"