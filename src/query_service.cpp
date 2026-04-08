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

namespace {
bool should_retry(YdbQueryRetrySettings *rs, uint32_t attempt) {
  if (!rs) {
    return false;
  }
  if (attempt >= rs->max_retries) {
    return false;
  }
  rs->current_retries = attempt + 1;
  if (rs->timeout_ms > 0) {
    usleep(static_cast<useconds_t>(rs->timeout_ms) * 1000);
  }
  return true;
}
} // namespace

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

struct ydb_retry_context{
  YdbQueryClient* qc;
  
};

ydb_status_t
ydb_query_execute(YdbQueryClient *qc, const char *yql, ydb_tx_mode_t tx_mode,
                  const YdbQueryParams *params, YdbResultSets **out_results,
                  YdbQueryRetrySettings *rs, YdbResultDetails *result_details) {
                    // move retry settings to begin_tx
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

  YdbResultSets *resultsets_out = nullptr;
  if (rs) {
    rs->current_retries = 0;
  }

  try {
    for (uint32_t attempt = 0;; ++attempt) {
      delete resultsets_out;
      resultsets_out = nullptr;

      auto session_result = qc->client->GetSession().GetValueSync();
      ydb_status_t code = ydb_fill_from_status(result_details, session_result);
      if (session_result.IsSuccess()) {
        auto session = session_result.GetSession();
        auto result =
            sdk_params.has_value()
                ? session.ExecuteQuery(yql, tx_control, *sdk_params)
                      .ExtractValueSync()
                : session.ExecuteQuery(yql, tx_control).ExtractValueSync();

        code = ydb_fill_from_status(result_details, result);
        if (result.IsSuccess()) {
          auto *rs_out = new (std::nothrow) YdbResultSets();
          if (!rs_out) {
            delete resultsets_out;
            return ydb_result_details_fail(result_details, YDB_ERR_INTERNAL,
                                           "failed to allocate result sets");
          }

          for (auto &rset : result.GetResultSets()) {
            auto *set = new (std::nothrow) YdbResultSet(std::move(rset));
            if (!set) {
              delete rs_out;
              return ydb_result_details_fail(result_details, YDB_ERR_INTERNAL,
                                             "failed to allocate a result set");
            }
            rs_out->sets.push_back(set);
          }

          resultsets_out = rs_out;
          break;
        }
      }

      if (!should_retry(rs, attempt)) {
        delete resultsets_out;
        return code;
      }
    }
  } catch (const std::exception &e) {
    delete resultsets_out;
    return ydb_result_details_fail(result_details, YDB_ERR_INTERNAL, e.what());
  }

  if (out_results) {
    *out_results = resultsets_out;
  } else {
    delete resultsets_out;
  }

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

} // extern "C"
