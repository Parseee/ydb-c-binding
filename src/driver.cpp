#include "internal.hpp"
#include "ydb.h"
#include "ydb_error.h"

#include <cstdint>
#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <ydb-cpp-sdk/client/value/value.h>

ydb_status_t status_to_ydb_code(NYdb::EStatus s) {
  switch (s) {
  case NYdb::EStatus::SUCCESS:
    return YDB_OK;
  case NYdb::EStatus::BAD_REQUEST:
    return YDB_ERR_BAD_REQUEST;
  case NYdb::EStatus::NOT_FOUND:
    return YDB_ERR_NOT_FOUND;
  case NYdb::EStatus::INTERNAL_ERROR:
    return YDB_ERR_INTERNAL;
  case NYdb::EStatus::TIMEOUT:
    return YDB_ERR_TIMEOUT;
  case NYdb::EStatus::ABORTED:
  case NYdb::EStatus::UNAVAILABLE:
  case NYdb::EStatus::OVERLOADED:
  case NYdb::EStatus::CLIENT_RESOURCE_EXHAUSTED:
  case NYdb::EStatus::CLIENT_DISCOVERY_FAILED:
  case NYdb::EStatus::SESSION_BUSY:
  case NYdb::EStatus::SESSION_EXPIRED:
  case NYdb::EStatus::TRANSPORT_UNAVAILABLE:
  case NYdb::EStatus::CLIENT_UNAUTHENTICATED:
    return YDB_ERR_CONNECTION;
  default:
    return YDB_ERR_GENERIC;
  }
}

extern "C" {

struct Version {
  uint32_t major_version;
  uint32_t minor_version;
  uint32_t patch_version;
  std::string comment;
};

static const Version YDB_SDK_VERSION = {0, 1, 3, "Nightly"};
const char *ydb_get_version() {
  static const std::string version_str =
      std::to_string(YDB_SDK_VERSION.major_version) + "." +
      std::to_string(YDB_SDK_VERSION.minor_version) + "." +
      std::to_string(YDB_SDK_VERSION.patch_version) + " (" +
      YDB_SDK_VERSION.comment + ")";

  return version_str.c_str();
}

YdbDriverConfig *ydb_driver_config_create(YdbResultDetails *rd) {
  CHECK_RD_PTR(rd);
  auto *cfg = new (std::nothrow) YdbDriverConfig();
  if (!cfg) {
    ydb_result_details_fail(rd, YDB_ERR_INTERNAL,
                            "failed to allocate driver config");
    return nullptr;
  }
  return cfg;
}
void ydb_driver_config_free(YdbDriverConfig *cfg) { delete cfg; }

ydb_status_t ydb_driver_config_set_endpoint(YdbDriverConfig *cfg, const char *v,
                                            YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!cfg || !v) {
    // return ydb_result_details_fail(rd, YDB_ERR_BAD_REQUEST,
    return RD(YDB_ERR_BAD_REQUEST, "failed to set set endpoint");
  }
  cfg->endpoint = v;
  return YDB_OK;
}
ydb_status_t ydb_driver_config_set_database(YdbDriverConfig *cfg, const char *v,
                                            YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!cfg || !v) {
    return RD(YDB_ERR_BAD_REQUEST, "failed to set database");
  }
  cfg->database = v;
  return YDB_OK;
}
ydb_status_t ydb_driver_config_set_auth_token(YdbDriverConfig *cfg,
                                              const char *v,
                                              YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!cfg || !v) {
    return RD(YDB_ERR_BAD_REQUEST, "failed to set auth token");
  }
  cfg->auth_token = v;
  return YDB_OK;
}

YdbDriver *ydb_driver_create(const YdbDriverConfig *cfg, YdbResultDetails *rd) {
  CHECK_RD_PTR(rd);

  if (!cfg) {
    ydb_result_details_fail(rd, YDB_ERR_INTERNAL, "Driver Config is not set");
    return nullptr;
  }

  auto *drv = new (std::nothrow) YdbDriver();
  if (!drv) {
    ydb_result_details_fail(rd, YDB_ERR_INTERNAL,
                            "Failed to allocate the driver");
    return nullptr;
  }

  try {
    auto c = NYdb::TDriverConfig()
                 .SetEndpoint(cfg->endpoint)
                 .SetDatabase(cfg->database)
                 .SetAuthToken(cfg->auth_token);
    drv->config = std::make_unique<NYdb::TDriverConfig>(std::move(c));
    drv->driver = std::make_unique<NYdb::TDriver>(*drv->config);
  } catch (const std::exception &e) {
    ydb_result_details_fail(rd, YDB_ERR_INTERNAL, e.what());
    delete drv;
    return nullptr;
  }
  return drv;
}

ydb_status_t ydb_driver_start(YdbDriver *drv, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!drv) {
    return RD(YDB_ERR_BAD_REQUEST, "driver is null");
  }
  return YDB_OK;
}

ydb_status_t ydb_driver_wait_ready(YdbDriver *drv, uint32_t timeout_ms,
                                   YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!drv) {
    return RD(YDB_ERR_BAD_REQUEST, "driver is null");
  }
  if (!drv->driver) {
    return RD(YDB_ERR_BAD_REQUEST, "driver handle is not initialized");
  }

  auto query_client = NYdb::NQuery::TQueryClient(*drv->driver);
  const auto started = std::chrono::steady_clock::now();
  const auto timeout = std::chrono::milliseconds(timeout_ms);

  for (;;) {
    auto session_result = query_client.GetSession().GetValueSync();
    if (session_result.IsSuccess()) {
      return YDB_OK;
    }

    (void)ydb_fill_from_status(rd, session_result);

    const auto elapsed = std::chrono::steady_clock::now() - started;
    if (elapsed >= timeout) {
      const std::string timeout_msg = "driver is not ready before timeout (" +
                                      std::to_string(timeout_ms) + " ms)";
      return ydb_result_details_fail(rd, YDB_ERR_TIMEOUT, timeout_msg.c_str());
    }
    // TODO: this thread? or busy loop?
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

void ydb_driver_free(YdbDriver *drv) {
  if (!drv) {
    return;
  }
  if (drv->driver) {
    drv->driver->Stop(true);
  }
  delete drv;
}

YdbQueryParams *ydb_query_params_create(YdbResultDetails *rd) {
  CHECK_RD_PTR(rd);

  auto *params = new (std::nothrow) YdbQueryParams();
  if (!params) {
    RD(YDB_ERR_INTERNAL, "failed to allocate query params");
    return nullptr;
  }
  return params;
}
void ydb_query_params_free(YdbQueryParams *p, YdbResultDetails *rd) {
  if (!p) {
    RD(YDB_ERR_BAD_REQUEST, "query params is null");
    return;
  }
  delete p;
}

ydb_status_t ydb_params_set_utf8(YdbQueryParams *p, const char *name,
                                 const char *value, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name || !value) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid utf8 parameter");
  }
  p->builder.AddParam(name).Utf8(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_int64(YdbQueryParams *p, const char *name,
                                  int64_t value, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid int64 parameter");
  }
  p->builder.AddParam(name).Int64(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_uint64(YdbQueryParams *p, const char *name,
                                   uint64_t value, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid uint64 parameter");
  }
  p->builder.AddParam(name).Uint64(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_double(YdbQueryParams *p, const char *name,
                                   double value, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid double parameter");
  }
  p->builder.AddParam(name).Double(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_bool(YdbQueryParams *p, const char *name, int value,
                                 YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bool parameter");
  }
  p->builder.AddParam(name).Bool(value != 0).Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_bytes(YdbQueryParams *p, const char *name,
                                  const void *data, size_t len,
                                  YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name || !data) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bytes parameter");
  }
  p->builder.AddParam(name)
      .String(std::string(static_cast<const char *>(data), len))
      .Build();
  return YDB_OK;
}

// ---------------- YdbParamBuilder --------------
YdbParamBuilder *ydb_params_begin_param(YdbQueryParams *p, const char *name,
                                        YdbResultDetails *rd) {
  if (!p || !name) {
    RD(YDB_ERR_BAD_REQUEST, "param builder requires name");
    return nullptr;
  }
  auto *b = new (std::nothrow) YdbParamBuilder();
  if (!b) {
    RD(YDB_ERR_INTERNAL, "failed to allocate param builder");
    return nullptr;
  }
  b->owner = p;
  b->slot = &p->builder.AddParam(name);
  return b;
}

ydb_status_t ydb_params_end_param(YdbParamBuilder *b, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b) {
    return RD(YDB_ERR_BAD_REQUEST, "param builder is null");
  }
  b->slot->Build();
  delete b;
  return YDB_OK;
}

ydb_status_t ydb_params_begin_list(YdbParamBuilder *b, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  b->slot->BeginList();
  return YDB_OK;
}

// TODO: add added element
ydb_status_t ydb_params_add_list_item(YdbParamBuilder *b,
                                      YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  b->slot->AddListItem();
  return YDB_OK;
}

ydb_status_t ydb_params_end_list(YdbParamBuilder *b, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  b->slot->EndList();
  return YDB_OK;
}

ydb_status_t ydb_params_begin_struct(YdbParamBuilder *b, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "struct builder is null");
  }
  b->slot->BeginStruct();
  return YDB_OK;
}

ydb_status_t ydb_params_end_struct(YdbParamBuilder *b, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "struct builder is null");
  }
  b->slot->EndStruct();
  return YDB_OK;
}

ydb_status_t ydb_params_add_member_bool(YdbParamBuilder *b, const char *field,
                                        int v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bool member");
  }
  b->slot->AddMember(field).Bool(v != 0);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_int32(YdbParamBuilder *b, const char *field,
                                         int32_t v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid int32 member");
  }
  b->slot->AddMember(field).Int32(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_uint32(YdbParamBuilder *b, const char *field,
                                          uint32_t v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid uint32 member");
  }
  b->slot->AddMember(field).Uint32(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_int64(YdbParamBuilder *b, const char *field,
                                         int64_t v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid int64 member");
  }
  b->slot->AddMember(field).Int64(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_uint64(YdbParamBuilder *b, const char *field,
                                          uint64_t v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid uint64 member");
  }
  b->slot->AddMember(field).Uint64(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_float(YdbParamBuilder *b, const char *field,
                                         float v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid float member");
  }
  b->slot->AddMember(field).Float(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_double(YdbParamBuilder *b, const char *field,
                                          double v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid double member");
  }
  b->slot->AddMember(field).Double(v);
  return YDB_OK;
}
// TODO: check for len and NULL-terminator
ydb_status_t ydb_params_add_member_utf8(YdbParamBuilder *b, const char *field,
                                        const char *v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field || !v) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid utf8 member");
  }
  b->slot->AddMember(field).Utf8(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_bytes(YdbParamBuilder *b, const char *field,
                                         const void *data, size_t len,
                                         YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field || !data) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bytes member");
  }
  b->slot->AddMember(field).String(
      std::string(static_cast<const char *>(data), len));
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_null(YdbParamBuilder *b, const char *field,
                                        YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid null member");
  }
  b->slot->AddMember(field).EmptyOptional();
  return YDB_OK;
}

// TODO: YDB can get results as stream without bufferization
// ws can get results line-by-line
// though we should not expose the method to the user
int ydb_resultsets_count(const YdbResultSets *rs, YdbResultDetails *rd) {
  CHECK_RD_INT(rd, -1);
  if (!rs) {
    return RD(YDB_ERR_BAD_REQUEST, "result sets is null");
  }
  return static_cast<int>(rs->sets.size());
}
YdbResultSet *ydb_resultsets_release(YdbResultSets *rs, int index,
                                     YdbResultDetails *rd) {
  CHECK_RD_PTR(rd);
  if (!rs || index < 0 || static_cast<size_t>(index) >= rs->sets.size()) {
    RD(YDB_ERR_BAD_REQUEST, "result set index is out of range");
    return nullptr;
  }
  return rs->sets[static_cast<size_t>(index)].release();
}
void ydb_resultsets_free(YdbResultSets *rs, YdbResultDetails *rd) { delete rs; }
int ydb_resultset_column_count(const YdbResultSet *rs, YdbResultDetails *rd) {
  CHECK_RD_INT(rd, -1);
  if (!rs) {
    return RD(YDB_ERR_BAD_REQUEST, "result set is null");
  }
  return static_cast<int>(rs->parser.ColumnsCount());
}

[[deprecated("will be added later")]]
const char *ydb_resultset_column_name(const YdbResultSet *rs, int col_index,
                                      YdbResultDetails *rd) {
  CHECK_RD_PTR(rd);
  if (!rs || col_index < 0 ||
      col_index >= static_cast<int>(rs->resultSet.GetColumnsMeta().size())) {
    RD(YDB_ERR_BAD_REQUEST, "invalid column index");
    return nullptr;
  }
  return rs->resultSet.GetColumnsMeta()[static_cast<size_t>(col_index)]
      .Name.c_str();
}

[[deprecated("will be added later")]]
ydb_type_t ydb_resultset_column_type(const YdbResultSet *rs, int col_index,
                                     YdbResultDetails *rd) {
  if (!rs || col_index < 0 || col_index >= rs->parser.ColumnsCount())
    return YDB_TYPE_UNKNOWN;

  const auto &type = rs->resultSet.GetColumnsMeta()[col_index].Type;
  NYdb::TTypeParser parser(type);

  if (parser.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
    parser.OpenOptional();
    return YDB_TYPE_OPTIONAL;
  }

  if (parser.GetKind() == NYdb::TTypeParser::ETypeKind::Primitive)
    return static_cast<ydb_type_t>(
        static_cast<uint32_t>(parser.GetPrimitive()));

  return YDB_TYPE_UNKNOWN;
}
int ydb_resultset_next_row(YdbResultSet *rs, YdbResultDetails *rd) {
  CHECK_RD_INT(rd, -1);
  if (!rs) {
    return RD(YDB_ERR_BAD_REQUEST, "result set is null");
  }
  return rs->parser.TryNextRow() ? 1 : 0;
}
int ydb_resultset_is_null(YdbResultSet *rs, int col, YdbResultDetails *rd) {
  CHECK_RD_INT(rd, -1);
  if (!rs || col < 0 || col >= static_cast<int>(rs->parser.ColumnsCount())) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid column index");
  }
  try {
    return rs->parser.ColumnParser(static_cast<size_t>(col)).IsNull() ? 1 : 0;
  } catch (const std::exception &e) {
    return RD(YDB_ERR_INTERNAL, e.what());
  }
}

ydb_status_t ydb_resultset_get_utf8(YdbResultSet *rs, int col, const char **out,
                                    size_t *out_len, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!rs || !out || !out_len || col < 0 ||
      col >= static_cast<int>(rs->parser.ColumnsCount())) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid utf8 getter arguments");
  }
  try {
    auto value =
        rs->parser.ColumnParser(static_cast<size_t>(col)).GetOptionalUtf8();
    if (!value.has_value()) {
      return RD(YDB_ERR_NOT_FOUND, "column value is null");
    }
    rs->scratch = *value;
    *out = rs->scratch.c_str();
    *out_len = rs->scratch.size();
    return YDB_OK;
  } catch (const std::exception &e) {
    return RD(YDB_ERR_INTERNAL, e.what());
  }
}
ydb_status_t ydb_resultset_get_int64(YdbResultSet *rs, int col, int64_t *out,
                                     YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!rs || !out || col < 0 ||
      col >= static_cast<int>(rs->parser.ColumnsCount())) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid int64 getter arguments");
  }
  try {
    auto value =
        rs->parser.ColumnParser(static_cast<size_t>(col)).GetOptionalInt64();
    if (!value.has_value()) {
      return RD(YDB_ERR_NOT_FOUND, "column value is null");
    }
    *out = *value;
    return YDB_OK;
  } catch (const std::exception &e) {
    return RD(YDB_ERR_INTERNAL, e.what());
  }
}
ydb_status_t ydb_resultset_get_uint64(YdbResultSet *rs, int col, uint64_t *out,
                                      YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!rs || !out || col < 0 ||
      col >= static_cast<int>(rs->parser.ColumnsCount())) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid uint64 getter arguments");
  }
  try {
    auto value =
        rs->parser.ColumnParser(static_cast<size_t>(col)).GetOptionalUint64();
    if (!value.has_value()) {
      return RD(YDB_ERR_NOT_FOUND, "column value is null");
    }
    *out = *value;
    return YDB_OK;
  } catch (const std::exception &e) {
    return RD(YDB_ERR_INTERNAL, e.what());
  }
}
ydb_status_t ydb_resultset_get_double(YdbResultSet *rs, int col, double *out,
                                      YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!rs || !out || col < 0 ||
      col >= static_cast<int>(rs->parser.ColumnsCount())) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid double getter arguments");
  }
  try {
    auto value =
        rs->parser.ColumnParser(static_cast<size_t>(col)).GetOptionalDouble();
    if (!value.has_value()) {
      return RD(YDB_ERR_NOT_FOUND, "column value is null");
    }
    *out = *value;
    return YDB_OK;
  } catch (const std::exception &e) {
    return RD(YDB_ERR_INTERNAL, e.what());
  }
}
ydb_status_t ydb_resultset_get_bool(YdbResultSet *rs, int col, int *out,
                                    YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!rs || !out || col < 0 ||
      col >= static_cast<int>(rs->parser.ColumnsCount())) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bool getter arguments");
  }
  try {
    auto value =
        rs->parser.ColumnParser(static_cast<size_t>(col)).GetOptionalBool();
    if (!value.has_value()) {
      return RD(YDB_ERR_NOT_FOUND, "column value is null");
    }
    *out = *value ? 1 : 0;
    return YDB_OK;
  } catch (const std::exception &e) {
    return RD(YDB_ERR_INTERNAL, e.what());
  }
}
ydb_status_t ydb_resultset_get_bytes(YdbResultSet *rs, int col,
                                     const void **out, size_t *out_len,
                                     YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!rs || !out || !out_len || col < 0 ||
      col >= static_cast<int>(rs->parser.ColumnsCount())) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bytes getter arguments");
  }
  try {
    auto value =
        rs->parser.ColumnParser(static_cast<size_t>(col)).GetOptionalBytes();
    if (!value.has_value()) {
      return RD(YDB_ERR_NOT_FOUND, "column value is null");
    }
    rs->scratch = *value;
    *out = rs->scratch.data();
    *out_len = rs->scratch.size();
    return YDB_OK;
  } catch (const std::exception &e) {
    return RD(YDB_ERR_INTERNAL, e.what());
  }
}

} // extern "C"
