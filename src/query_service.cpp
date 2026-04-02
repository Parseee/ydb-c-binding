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
#include <utility>

extern "C" {

YdbQueryClient *ydb_query_client_create(YdbDriver *drv,
                                        ydb_result_details_t *rd) {
  if (!drv || !drv->driver) {
    ydb_result_details_fail(rd, YDB_ERR_BAD_REQUEST, "driver is null");
    return nullptr;
  }

  auto *qc = new (std::nothrow) YdbQueryClient();
  if (!qc) {
    ydb_result_details_fail(rd, YDB_ERR_INTERNAL,
                            "failed to allocate query client");
    return nullptr;
  }

  try {
    qc->client = std::make_unique<NYdb::NQuery::TQueryClient>(*drv->driver);
    qc->parent_driver = drv;
  } catch (const std::exception &e) {
    ydb_result_details_fail(rd, YDB_ERR_INTERNAL, e.what());
    delete qc;
    return nullptr;
  }
  return qc;
}
void ydb_query_client_free(YdbQueryClient *qc) {
  delete qc;
}

ydb_status_t ydb_query_begin_tx(YdbQueryClient *qc, ydb_tx_mode_t tx_mode,
                                YdbQueryTransaction **out_tx,
                                ydb_result_details_t *rd) {
  if (!qc || !out_tx) {
    return ydb_result_details_fail(rd, YDB_ERR_BAD_REQUEST,
                                   "query client or out_tx is null");
  }

  *out_tx = nullptr;
  if (tx_mode == YDB_TX_NONE) {
    return ydb_result_details_fail(rd, YDB_ERR_BAD_REQUEST,
                                   "transaction mode must not be NONE");
  }

  NYdb::NQuery::TTxSettings settings;
  switch (tx_mode) {
  case YDB_TX_SERIALIZABLE_RW:
    settings = NYdb::NQuery::TTxSettings::SerializableRW();
    break;
  case YDB_TX_SNAPSHOT_RO:
    settings = NYdb::NQuery::TTxSettings::SnapshotRO();
    break;
  case YDB_TX_STALE_RO:
    settings = NYdb::NQuery::TTxSettings::StaleRO();
    break;
  case YDB_TX_ONLINE_RO:
    settings = NYdb::NQuery::TTxSettings::OnlineRO();
    break;
  case YDB_TX_SNAPSHOT_RW:
    settings = NYdb::NQuery::TTxSettings::SnapshotRW();
    break;
  default:
    return ydb_result_details_fail(rd, YDB_ERR_BAD_REQUEST,
                                   "unsupported transaction mode");
  }

  auto session_result = qc->client->GetSession().GetValueSync();
  if (!session_result.IsSuccess()) {
    return ydb_fill_from_status(rd, session_result);
  }

  auto session = session_result.GetSession();
  auto tx_result = session.BeginTransaction(settings).GetValueSync();
  if (!tx_result.IsSuccess()) {
    return ydb_fill_from_status(rd, tx_result);
  }

  auto tx = tx_result.GetTransaction();
  auto *wrapped = new (std::nothrow)
      YdbQueryTransaction(std::move(session), std::move(tx));
  if (!wrapped) {
    return ydb_result_details_fail(rd, YDB_ERR_INTERNAL,
                                   "failed to allocate query transaction");
  }

  *out_tx = wrapped;
  return YDB_OK;
}

ydb_status_t ydb_query_execute(YdbQueryClient *qc, const char *yql,
                               ydb_tx_mode_t tx_mode,
                               const YdbQueryParams *params,
                               YdbResultSets **out_results,
                               ydb_result_details_t *result_details) {
  if (!qc || !yql) {
    return ydb_result_details_fail(result_details, YDB_ERR_BAD_REQUEST,
                                   "query client or yql is null");
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

          ydb_fill_from_status(result_details, result);
          if (!result.IsSuccess()) {
            return result;
          }

          // Collect all result sets from the successful execution.
          auto *rs = new (std::nothrow) YdbResultSets();
          if (!rs) {
            ydb_result_details_fail(result_details, YDB_ERR_INTERNAL,
                                    "failed to allocate result sets");
            return NYdb::TStatus(NYdb::EStatus::CLIENT_OUT_OF_RANGE,
                                 NYdb::NIssue::TIssues{});
          }

          for (auto &rset : result.GetResultSets()) {
            auto *set = new (std::nothrow) YdbResultSet(std::move(rset));
            if (!set) {
              delete rs;
              ydb_result_details_fail(result_details, YDB_ERR_INTERNAL,
                                      "failed to allocate a result set");
              return NYdb::TStatus(NYdb::EStatus::CLIENT_OUT_OF_RANGE,
                                   NYdb::NIssue::TIssues{});
            }
          }

          rs_out = rs;
          return result;
        });

    if (!status.IsSuccess()) {
      delete rs_out;
      return ydb_fill_from_status(result_details, status);
    }
  } catch (const std::exception &e) {
    delete rs_out;
    return ydb_result_details_fail(result_details, YDB_ERR_INTERNAL, e.what());
  }

  if (out_results) {
    *out_results = rs_out;
  } else {
    delete rs_out;
  }

  return YDB_OK;
}

ydb_status_t ydb_query_tx_execute(YdbQueryTransaction *tx, const char *yql,
                                  const YdbQueryParams *params,
                                  YdbResultSets **out_results,
                                  ydb_result_details_t *result_details) {
  if (!tx || !yql) {
    return ydb_result_details_fail(result_details, YDB_ERR_BAD_REQUEST,
                                   "transaction or yql is null");
  }
  if (!tx->tx.IsActive()) {
    return ydb_result_details_fail(result_details, YDB_ERR_BAD_REQUEST,
                                   "transaction is not active");
  }

  std::optional<NYdb::TParams> sdk_params;
  if (params) {
    sdk_params = const_cast<YdbQueryParams *>(params)->builder.Build();
  }

  auto tx_control = NYdb::NQuery::TTxControl::Tx(tx->tx);
  auto result =
      sdk_params.has_value()
          ? tx->session.ExecuteQuery(yql, tx_control, *sdk_params)
                .ExtractValueSync()
          : tx->session.ExecuteQuery(yql, tx_control).ExtractValueSync();

  const ydb_status_t code = ydb_fill_from_status(result_details, result);
  if (!result.IsSuccess()) {
    return code;
  }

  auto *rs = new (std::nothrow) YdbResultSets();
  if (!rs) {
    return ydb_result_details_fail(result_details, YDB_ERR_INTERNAL,
                                   "failed to allocate result sets");
  }

  for (auto &rset : result.GetResultSets()) {
    auto *set = new (std::nothrow) YdbResultSet(std::move(rset));
    if (!set) {
      delete rs;
      return ydb_result_details_fail(result_details, YDB_ERR_INTERNAL,
                                     "failed to allocate a result set");
    }
    rs->sets.push_back(set);
  }

  if (out_results) {
    *out_results = rs;
  } else {
    delete rs;
  }

  return YDB_OK;
}
ydb_status_t ydb_query_tx_commit(YdbQueryTransaction *tx,
                                 ydb_result_details_t *result_details) {
  if (!tx) {
    return ydb_result_details_fail(result_details, YDB_ERR_BAD_REQUEST,
                                   "transaction is null");
  }
  if (!tx->tx.IsActive()) {
    return ydb_result_details_fail(result_details, YDB_ERR_BAD_REQUEST,
                                   "transaction is not active");
  }
  auto result = tx->tx.Commit().GetValueSync();
  return ydb_fill_from_status(result_details, result);
}
ydb_status_t ydb_query_tx_rollback(YdbQueryTransaction *tx,
                                   ydb_result_details_t *result_details) {
  if (!tx) {
    return ydb_result_details_fail(result_details, YDB_ERR_BAD_REQUEST,
                                   "transaction is null");
  }
  if (!tx->tx.IsActive()) {
    return ydb_result_details_fail(result_details, YDB_ERR_BAD_REQUEST,
                                   "transaction is not active");
  }
  auto result = tx->tx.Rollback().GetValueSync();
  return ydb_fill_from_status(result_details, result);
}
void ydb_query_tx_free(YdbQueryTransaction *tx,
                       ydb_result_details_t *result_details) {
  if (!tx) {
    ydb_result_details_fail(result_details, YDB_ERR_BAD_REQUEST,
                            "transaction is null");
    return;
  }
  delete tx;
}

} // extern "C"
