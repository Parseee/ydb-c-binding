#include "include/internal.h"
#include "ydb.h"

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <memory>
#include <string>
#include <ydb-cpp-sdk/client/value/value.h>

thread_local std::string g_last_error;
void set_last_error(const std::string &msg) { g_last_error = msg; }
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

static const Version YDB_SDK_VERSION = {0, 1, 0, "Nightly"};
const char *ydb_get_version() {
  static const std::string version_str =
      std::to_string(YDB_SDK_VERSION.major_version) + "." +
      std::to_string(YDB_SDK_VERSION.minor_version) + "." +
      std::to_string(YDB_SDK_VERSION.patch_version) + " (" +
      YDB_SDK_VERSION.comment + ")";

  return version_str.c_str();
}

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

YdbQueryParams *ydb_query_params_create(void) {
  return new (std::nothrow) YdbQueryParams();
}
void ydb_query_params_free(YdbQueryParams *p) { delete p; }

ydb_status_t ydb_params_set_utf8(YdbQueryParams *p, const char *name,
                                 const char *value) {
  if (!p || !name || !value)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name).Utf8(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_int64(YdbQueryParams *p, const char *name,
                                  int64_t value) {
  if (!p || !name)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name).Int64(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_uint64(YdbQueryParams *p, const char *name,
                                   uint64_t value) {
  if (!p || !name)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name).Uint64(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_double(YdbQueryParams *p, const char *name,
                                   double value) {
  if (!p || !name)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name).Double(value).Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_bool(YdbQueryParams *p, const char *name,
                                 int value) {
  if (!p || !name)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name).Bool(value != 0).Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_bytes(YdbQueryParams *p, const char *name,
                                  const void *data, size_t len) {
  if (!p || !name || !data)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name)
      .String(std::string(static_cast<const char *>(data), len))
      .Build();
  return YDB_OK;
}
ydb_status_t ydb_params_set_decimal(YdbQueryParams *p, const char *name,
                                    const char *value, uint8_t precision,
                                    uint8_t scale) {
  if (!p || !name || !value)
    return YDB_ERR_BAD_REQUEST;
  p->builder.AddParam(name)
      .Decimal(NYdb::TDecimalValue(value, precision, scale))
      .Build();
  return YDB_OK;
}

// ---------------- YdbParamBuilder --------------
YdbParamBuilder *ydb_params_begin_param(YdbQueryParams *p, const char *name) {
  if (!p || !name) {
    return nullptr;
  }
  auto *b = new (std::nothrow) YdbParamBuilder();
  if (!b) {
    return nullptr;
  }
  b->owner = p;
  b->slot = &p->builder.AddParam(name);
  return b;
}

ydb_status_t ydb_params_end_param(YdbParamBuilder *b) {
  if (!b) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->Build();
  delete b;
  return YDB_OK;
}

ydb_status_t ydb_params_begin_list(YdbParamBuilder *b) {
  if (!b || !b->slot) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->BeginList();
  return YDB_OK;
}

ydb_status_t ydb_params_add_list_item(YdbParamBuilder *b) {
  if (!b || !b->slot) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddListItem();
  return YDB_OK;
}

ydb_status_t ydb_params_end_list(YdbParamBuilder *b) {
  if (!b || !b->slot) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->EndList();
  return YDB_OK;
}

ydb_status_t ydb_params_begin_struct(YdbParamBuilder *b) {
  if (!b || !b->slot) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->BeginStruct();
  return YDB_OK;
}

ydb_status_t ydb_params_end_struct(YdbParamBuilder *b) {
  if (!b || !b->slot) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->EndStruct();
  return YDB_OK;
}

ydb_status_t ydb_params_add_member_bool(YdbParamBuilder *b, const char *field,
                                        int v) {
  if (!b || !field) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).Bool(v != 0);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_int32(YdbParamBuilder *b, const char *field,
                                         int32_t v) {
  if (!b || !field) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).Int32(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_uint32(YdbParamBuilder *b, const char *field,
                                          uint32_t v) {
  if (!b || !field) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).Uint32(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_int64(YdbParamBuilder *b, const char *field,
                                         int64_t v) {
  if (!b || !field) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).Int64(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_uint64(YdbParamBuilder *b, const char *field,
                                          uint64_t v) {
  if (!b || !field) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).Uint64(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_float(YdbParamBuilder *b, const char *field,
                                         float v) {
  if (!b || !field) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).Float(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_double(YdbParamBuilder *b, const char *field,
                                          double v) {
  if (!b || !field) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).Double(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_utf8(YdbParamBuilder *b, const char *field,
                                        const char *v) {
  if (!b || !field || !v) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).Utf8(v);
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_bytes(YdbParamBuilder *b, const char *field,
                                         const void *data, size_t len) {
  if (!b || !field || !data) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).String(
      std::string(static_cast<const char *>(data), len));
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_null(YdbParamBuilder *b, const char *field) {
  if (!b || !field) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).EmptyOptional();
  return YDB_OK;
}
ydb_status_t ydb_params_add_member_decimal(YdbParamBuilder *b,
                                           const char *field, const char *value,
                                           uint8_t precision, uint8_t scale) {
  if (!b || !field || !value) {
    return YDB_ERR_BAD_REQUEST;
  }
  b->slot->AddMember(field).Decimal(
      NYdb::TDecimalValue(value, precision, scale));
  return YDB_OK;
}

int ydb_result_sets_count(const YdbResultSets *) { return 0; }
YdbResultSet *ydb_result_sets_get(YdbResultSets *, int) { return nullptr; }
void ydb_result_sets_free(YdbResultSets *rs) { delete rs; }

int ydb_result_set_column_count(const YdbResultSet *) { return 0; }
const char *ydb_result_set_column_name(const YdbResultSet *, int) {
  return nullptr;
}
ydb_type_t ydb_result_set_column_type(const YdbResultSet *rs, int col_index) {
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