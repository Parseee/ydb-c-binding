#include "include/internal.hpp"
#include "ydb.h"
#include "ydb_error.h"

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <memory>
#include <string>
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
  // TODO: workaround for funcs that return non status codes
  if (isFatal(rd)) {
    std::string error_msg = std::string("from ") + __func__;
    ydb_result_details_append_message(rd, error_msg.c_str());
    return nullptr;
  }
  return new (std::nothrow) YdbDriverConfig();
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

YdbDriver *ydb_driver_create(const YdbDriverConfig *cfg,
                             YdbResultDetails *rd) {
  // TODO: workaround for funcs that return non status codes
  if (isFatal(rd)) {
    std::string error_msg = std::string("from ") + __func__;
    ydb_result_details_append_message(rd, error_msg.c_str());
    return nullptr;
  }

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
    // ydb_result_details_print(e.what());
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

ydb_status_t ydb_driver_wait_ready(YdbDriver *drv, int,
                                   YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!drv) {
    return RD(YDB_ERR_BAD_REQUEST, "driver is null");
  }
  return YDB_OK;
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
  // TODO: workaround for funcs that return non status codes
  if (isFatal(rd)) {
    std::string error_msg = std::string("from ") + __func__;
    ydb_result_details_append_message(rd, error_msg.c_str());
    return nullptr;
  }

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

ydb_status_t ydb_params_end_param(YdbParamBuilder *b,
                                  YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b) {
    return RD(YDB_ERR_BAD_REQUEST, "param builder is null");
  }
  b->slot->Build();
  delete b;
  return YDB_OK;
}

ydb_status_t ydb_params_begin_list(YdbParamBuilder *b,
                                   YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  b->slot->BeginList();
  return YDB_OK;
}

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

ydb_status_t ydb_params_begin_struct(YdbParamBuilder *b,
                                     YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "struct builder is null");
  }
  b->slot->BeginStruct();
  return YDB_OK;
}

ydb_status_t ydb_params_end_struct(YdbParamBuilder *b,
                                   YdbResultDetails *rd) {
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
                                          uint32_t v,
                                          YdbResultDetails *rd) {
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
                                          uint64_t v,
                                          YdbResultDetails *rd) {
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
ydb_status_t ydb_params_add_member_utf8(YdbParamBuilder *b, const char *field,
                                        const char *v,
                                        YdbResultDetails *rd) {
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

// we do not know about the count before the whole read
int ydb_resultsets_count(const YdbResultSets *, YdbResultDetails *rd) {
  CHECK_RD(rd);
  return 0; // TODO: does nothing
}
YdbResultSet *ydb_resultsets_get(YdbResultSets *, int,
                                 YdbResultDetails *rd) {
  return nullptr; // TODO: does nothing
}
void ydb_resultsets_free(YdbResultSets *rs, YdbResultDetails *rd) {
  delete rs;
}
int ydb_resultset_column_count(const YdbResultSet *, YdbResultDetails *rd) {
  return 0;
}
const char *ydb_resultset_column_name(const YdbResultSet *, int,
                                      YdbResultDetails *rd) {
  return nullptr;
}
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
int ydb_resultset_next_row(YdbResultSet *, YdbResultDetails *rd) {
  return 0;
}
int ydb_resultset_is_null(YdbResultSet *, int, YdbResultDetails *rd) {
  return 1;
}

ydb_status_t ydb_resultset_get_utf8(YdbResultSet *, int, const char **,
                                    size_t *, YdbResultDetails *rd) {
  CHECK_RD(rd);
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_resultset_get_int64(YdbResultSet *, int, int64_t *,
                                     YdbResultDetails *rd) {
  CHECK_RD(rd);
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_resultset_get_uint64(YdbResultSet *, int, uint64_t *,
                                      YdbResultDetails *rd) {
  CHECK_RD(rd);
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_resultset_get_double(YdbResultSet *, int, double *,
                                      YdbResultDetails *rd) {
  CHECK_RD(rd);
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_resultset_get_bool(YdbResultSet *, int, int *,
                                    YdbResultDetails *rd) {
  CHECK_RD(rd);
  return YDB_ERR_GENERIC;
}
ydb_status_t ydb_resultset_get_bytes(YdbResultSet *, int, const void **,
                                     size_t *, YdbResultDetails *rd) {
  CHECK_RD(rd);
  return YDB_ERR_GENERIC;
}

} // extern "C"
