#include "internal.hpp"
#include "ydb.h"
#include "ydb_error.h"

#include <cstdint>
#include <unistd.h>
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

YdbQueryClient *ydb_query_client_create(YdbDriver *drv, YdbResultDetails *rd) {
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
void ydb_query_client_free(YdbQueryClient *qc) { delete qc; }

// DDL
ydb_status_t ydb_query_NOtx_execute(YdbQueryClient *qc, const char *yql,
                                    const YdbQueryParams *params,
                                    YdbResultSets **out_results,
                                    YdbResultDetails *result_details) {
  if (!qc || !yql) {
    return ydb_result_details_fail(result_details, YDB_ERR_BAD_REQUEST,
                                   "query client or yql is null");
  }

  std::optional<NYdb::TParams> sdk_params;
  if (params) {
    sdk_params = const_cast<YdbQueryParams *>(params)->builder.Build();
  }

  auto resultsets_out = std::make_unique<YdbResultSets>();

  try {
    auto session_result = qc->client->GetSession().GetValueSync();
    ydb_status_t code = ydb_fill_from_status(result_details, session_result);
    if (session_result.IsSuccess()) {
      auto session = session_result.GetSession();
      auto result =
          sdk_params.has_value()
              ? session
                    .ExecuteQuery(yql, NYdb::NQuery::TTxControl::NoTx(),
                                  *sdk_params)
                    .ExtractValueSync()
              : session.ExecuteQuery(yql, NYdb::NQuery::TTxControl::NoTx())
                    .ExtractValueSync();

      code = ydb_fill_from_status(result_details, result);
      if (result.IsSuccess()) {
        for (auto &rset : result.GetResultSets()) {
          resultsets_out->sets.push_back(std::make_unique<YdbResultSet>(rset));
        }
      }
    }
  } catch (const std::exception &e) {
    return ydb_result_details_fail(result_details, YDB_ERR_INTERNAL, e.what());
  }

  if (out_results) {
    *out_results = resultsets_out.release();
  } else {
    ;
  }

  return YDB_OK;
}

ydb_status_t ydb_query_begin_tx(YdbQueryClient *qc, ydb_tx_mode_t tx_mode,
                                YdbQueryTransaction **out_tx,
                                YdbResultDetails *rd) {
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
  auto *wrapped =
      new (std::nothrow) YdbQueryTransaction(std::move(session), std::move(tx));
  if (!wrapped) {
    return ydb_result_details_fail(rd, YDB_ERR_INTERNAL,
                                   "failed to allocate query transaction");
  }

  *out_tx = wrapped;
  return YDB_OK;
}

ydb_status_t ydb_query_tx_execute(YdbQueryTransaction *tx, const char *yql,
                                  const YdbQueryParams *params,
                                  YdbResultSets **out_results,
                                  YdbResultDetails *result_details) {
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
  auto result_sets = std::make_unique<YdbResultSets>();

  try {
    auto result =
        sdk_params.has_value()
            ? tx->session.ExecuteQuery(yql, tx_control, *sdk_params)
                  .ExtractValueSync()
            : tx->session.ExecuteQuery(yql, tx_control).ExtractValueSync();

    const ydb_status_t code = ydb_fill_from_status(result_details, result);
    if (!result.IsSuccess()) {
      return code;
    }

    for (auto &rset : result.GetResultSets()) {
      result_sets->sets.push_back(std::make_unique<YdbResultSet>(rset));
    }
  } catch (const std::exception &e) {
    return ydb_result_details_fail(result_details, YDB_ERR_INTERNAL, e.what());
  }

  if (out_results) {
    *out_results = result_sets.release();
  } else {
    ;
  }

  return YDB_OK;
}
ydb_status_t ydb_query_tx_commit(YdbQueryTransaction *tx,
                                 YdbResultDetails *result_details) {
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
                                   YdbResultDetails *result_details) {
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
                       YdbResultDetails *result_details) {
  if (!tx) {
    ydb_result_details_fail(result_details, YDB_ERR_BAD_REQUEST,
                            "transaction is null");
    return;
  }
  delete tx;
}

YdbQueryRetrySettings *ydb_query_retry_settings_create(uint32_t max_retries,
                                                       uint32_t timeout_ms,
                                                       YdbResultDetails *rd) {
  auto *rs = new (std::nothrow) YdbQueryRetrySettings();
  if (!rs) {
    ydb_result_details_fail(rd, YDB_ERR_INTERNAL,
                            "failed to allocate retry settings");
    return nullptr;
  }
  rs->max_retries = max_retries;
  rs->current_retries = 0;
  rs->timeout_ms = timeout_ms;
  return rs;
}

void ydb_query_retry_settings_free(YdbQueryRetrySettings *rs,
                                   YdbResultDetails *rd) {
  (void)rd;
  delete rs;
}

ydb_status_t ydb_query_perform_retry(YdbQueryRetrySettings *rs,
                                     YdbResultDetails *rd) {
  if (!rs) {
    return ydb_result_details_fail(rd, YDB_ERR_BAD_REQUEST,
                                   "retry settings is null");
  }

  if (!ydb_is_status_retriable(rd->code)) {
    return ydb_result_details_fail(rd, YDB_ERR_RETRY_FAILED,
                                   "retry conditions are not met");
  }

  if (rs->current_retries >= rs->max_retries) {
    return ydb_result_details_fail(rd, YDB_ERR_RETRY_FAILED,
                                   "retry limit exceeded");
  }

  if (rs->timeout_ms > 0) {
    usleep(static_cast<useconds_t>(rs->timeout_ms) * 1000);
  }
  rs->current_retries += 1;
  return YDB_OK;
}

} // extern "C"
