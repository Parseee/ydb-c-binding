#include "internal.hpp"
#include "ydb.h"
#include "ydb_error.h"

#include <cstdint>
#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <ydb-cpp-sdk/client/value/value.h>

// i think macros are viable here bc they are now being injected into code
// and provide fast return without too much if()
#define RD(code, details) ydb_rd_fail(rd, code, details)
#define CHECK_RD(rd)                                                           \
  do {                                                                         \
    if (const auto early = ydb_check_rd_status((rd), __func__);                \
        early.has_value() && early.value() != YDB_OK) {                        \
      return *early;                                                           \
    }                                                                          \
  } while (0)
#define CHECK_RD_PTR(rd)                                                       \
  do {                                                                         \
    if (ydb_check_rd_fatal((rd), __func__)) {                                  \
      return nullptr;                                                          \
    }                                                                          \
  } while (0)
#define CHECK_RD_INT(rd, error_ret)                                            \
  do {                                                                         \
    if (ydb_check_rd_fatal((rd), __func__)) {                                  \
      return (error_ret);                                                      \
    }                                                                          \
  } while (0)
#define CHECK_RD_VOID(rd)                                                      \
  do {                                                                         \
    if (ydb_check_rd_fatal((rd), __func__)) {                                  \
      return;                                                                  \
    }                                                                          \
  } while (0)
#define CATCH_ALL_STATUS()                                                     \
  catch (const std::exception &e) {                                            \
    return RD(YDB_ERR_INTERNAL, e.what());                                     \
  } catch (...) {                                                              \
    return RD(YDB_ERR_INTERNAL, "uncaught C++ exception");                     \
  }
#define CATCH_ALL_PTR(rd)                                                      \
  catch (const std::exception &e) {                                            \
    ydb_result_details_fail((rd), YDB_ERR_INTERNAL, e.what());                 \
    return nullptr;                                                            \
  } catch (...) {                                                              \
    ydb_result_details_fail((rd), YDB_ERR_INTERNAL,                            \
                            "uncaught C++ exception");                         \
    return nullptr;                                                            \
  }
#define CATCH_ALL_INT(rd, fallback)                                            \
  catch (const std::exception &e) {                                            \
    ydb_result_details_fail((rd), YDB_ERR_INTERNAL, e.what());                 \
    return (fallback);                                                         \
  } catch (...) {                                                              \
    ydb_result_details_fail((rd), YDB_ERR_INTERNAL,                            \
                            "uncaught C++ exception");                         \
    return (fallback);                                                         \
  }
#define CATCH_ALL_VOID(rd)                                                     \
  catch (const std::exception &e) {                                            \
    ydb_result_details_fail((rd), YDB_ERR_INTERNAL, e.what());                 \
    return;                                                                    \
  } catch (...) {                                                              \
    ydb_result_details_fail((rd), YDB_ERR_INTERNAL,                            \
                            "uncaught C++ exception");                         \
    return;                                                                    \
  }

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
  try {
    static const std::string version_str =
        std::to_string(YDB_SDK_VERSION.major_version) + "." +
        std::to_string(YDB_SDK_VERSION.minor_version) + "." +
        std::to_string(YDB_SDK_VERSION.patch_version) + " (" +
        YDB_SDK_VERSION.comment + ")";
    return version_str.c_str();
  } catch (...) {
    return "unknown";
  }
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
  try {
    cfg->endpoint = v;
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_driver_config_set_database(YdbDriverConfig *cfg, const char *v,
                                            YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!cfg || !v) {
    return RD(YDB_ERR_BAD_REQUEST, "failed to set database");
  }
  try {
    cfg->database = v;
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_driver_config_set_auth_token(YdbDriverConfig *cfg,
                                              const char *v,
                                              YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!cfg || !v) {
    return RD(YDB_ERR_BAD_REQUEST, "failed to set auth token");
  }
  try {
    cfg->auth_token = v;
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
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

  try {
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
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  CATCH_ALL_STATUS();
}

void ydb_driver_free(YdbDriver *drv) {
  try {
    if (!drv) {
      return;
    }
    if (drv->driver) {
      drv->driver->Stop(true);
    }
    delete drv;
  }
  catch (...) {
    // C boundary must not throw.
  }
}

YdbQueryParams *ydb_query_params_create(YdbResultDetails *rd) {
  CHECK_RD_PTR(rd);
  try {
    auto *params = new (std::nothrow) YdbQueryParams();
    if (!params) {
      RD(YDB_ERR_INTERNAL, "failed to allocate query params");
      return nullptr;
    }
    return params;
  }
  CATCH_ALL_PTR(rd);
}
void ydb_query_params_free(YdbQueryParams *p, YdbResultDetails *rd) {
  try {
    if (!p) {
      RD(YDB_ERR_BAD_REQUEST, "query params is null");
      return;
    }
    delete p;
  } catch (...) {
    ydb_result_details_fail(rd, YDB_ERR_INTERNAL, "uncaught C++ exception");
  }
}

ydb_status_t ydb_params_set_utf8(YdbQueryParams *p, const char *name,
                                 const char *value, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name || !value) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid utf8 parameter");
  }
  try {
    p->builder.AddParam(name).Utf8(value).Build();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_set_int64(YdbQueryParams *p, const char *name,
                                  int64_t value, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid int64 parameter");
  }
  try {
    p->builder.AddParam(name).Int64(value).Build();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_set_uint64(YdbQueryParams *p, const char *name,
                                   uint64_t value, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid uint64 parameter");
  }
  try {
    p->builder.AddParam(name).Uint64(value).Build();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_set_double(YdbQueryParams *p, const char *name,
                                   double value, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid double parameter");
  }
  try {
    p->builder.AddParam(name).Double(value).Build();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_set_bool(YdbQueryParams *p, const char *name, int value,
                                 YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bool parameter");
  }
  try {
    p->builder.AddParam(name).Bool(value != 0).Build();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_set_bytes(YdbQueryParams *p, const char *name,
                                  const void *data, size_t len,
                                  YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!p || !name || !data) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bytes parameter");
  }
  try {
    p->builder.AddParam(name)
        .String(std::string(static_cast<const char *>(data), len))
        .Build();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

// ---------------- YdbParamBuilder --------------
YdbParamBuilder *ydb_params_begin_param(YdbQueryParams *p, const char *name,
                                        YdbResultDetails *rd) {
  if (!p || !name) {
    RD(YDB_ERR_BAD_REQUEST, "param builder requires name");
    return nullptr;
  }
  try {
    auto b = std::make_unique<YdbParamBuilder>();
    b->owner = p;
    b->slot = &p->builder.AddParam(name);
    return b.release();
  }
  CATCH_ALL_PTR(rd);
}

ydb_status_t ydb_params_end_param(YdbParamBuilder *b, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b) {
    return RD(YDB_ERR_BAD_REQUEST, "param builder is null");
  }
  try {
    b->slot->Build();
    delete b;
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_begin_list(YdbParamBuilder *b, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->BeginList();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item(YdbParamBuilder *b,
                                      YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->AddListItem();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item_bool(YdbParamBuilder *b, int v,
                                           YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->AddListItem().Bool(v != 0);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item_int32(YdbParamBuilder *b, int32_t v,
                                            YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->AddListItem().Int32(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item_uint32(YdbParamBuilder *b, uint32_t v,
                                             YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->AddListItem().Uint32(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item_int64(YdbParamBuilder *b, int64_t v,
                                            YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->AddListItem().Int64(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item_uint64(YdbParamBuilder *b, uint64_t v,
                                             YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->AddListItem().Uint64(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item_float(YdbParamBuilder *b, float v,
                                            YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->AddListItem().Float(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item_double(YdbParamBuilder *b, double v,
                                             YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->AddListItem().Double(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item_utf8(YdbParamBuilder *b, const char *v,
                                           YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot || !v) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid utf8 list item");
  }
  constexpr size_t kMaxUtf8Len = 1U << 20; // 1 MiB safety bound
  const void *terminator = std::memchr(v, '\0', kMaxUtf8Len + 1);
  if (!terminator) {
    return RD(YDB_ERR_BAD_REQUEST,
              "invalid utf8 list item: missing null terminator or too long");
  }
  try {
    const size_t len =
        static_cast<size_t>(static_cast<const char *>(terminator) - v);
    b->slot->AddListItem().Utf8(std::string(v, len));
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item_bytes(YdbParamBuilder *b,
                                            const void *data, size_t len,
                                            YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot || !data) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bytes list item");
  }
  try {
    b->slot->AddListItem().String(
        std::string(static_cast<const char *>(data), len));
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_list_item_null(YdbParamBuilder *b,
                                           YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->AddListItem().EmptyOptional();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_end_list(YdbParamBuilder *b, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "list builder is null");
  }
  try {
    b->slot->EndList();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_begin_struct(YdbParamBuilder *b, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "struct builder is null");
  }
  try {
    b->slot->BeginStruct();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_end_struct(YdbParamBuilder *b, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !b->slot) {
    return RD(YDB_ERR_BAD_REQUEST, "struct builder is null");
  }
  try {
    b->slot->EndStruct();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

ydb_status_t ydb_params_add_member_bool(YdbParamBuilder *b, const char *field,
                                        int v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bool member");
  }
  try {
    b->slot->AddMember(field).Bool(v != 0);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_add_member_int32(YdbParamBuilder *b, const char *field,
                                         int32_t v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid int32 member");
  }
  try {
    b->slot->AddMember(field).Int32(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_add_member_uint32(YdbParamBuilder *b, const char *field,
                                          uint32_t v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid uint32 member");
  }
  try {
    b->slot->AddMember(field).Uint32(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_add_member_int64(YdbParamBuilder *b, const char *field,
                                         int64_t v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid int64 member");
  }
  try {
    b->slot->AddMember(field).Int64(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_add_member_uint64(YdbParamBuilder *b, const char *field,
                                          uint64_t v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid uint64 member");
  }
  try {
    b->slot->AddMember(field).Uint64(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_add_member_float(YdbParamBuilder *b, const char *field,
                                         float v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid float member");
  }
  try {
    b->slot->AddMember(field).Float(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_add_member_double(YdbParamBuilder *b, const char *field,
                                          double v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid double member");
  }
  try {
    b->slot->AddMember(field).Double(v);
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_add_member_utf8(YdbParamBuilder *b, const char *field,
                                        const char *v, YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field || !v) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid utf8 member");
  }
  constexpr size_t kMaxUtf8Len = 1U << 20; // 1 MiB safety bound
  const void *terminator = std::memchr(v, '\0', kMaxUtf8Len + 1);
  if (!terminator) {
    return RD(YDB_ERR_BAD_REQUEST,
              "invalid utf8 member: missing null terminator or too long");
  }

  try {
    const size_t len =
        static_cast<size_t>(static_cast<const char *>(terminator) - v);
    b->slot->AddMember(field).Utf8(std::string(v, len));
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_add_member_bytes(YdbParamBuilder *b, const char *field,
                                         const void *data, size_t len,
                                         YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field || !data) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid bytes member");
  }
  try {
    b->slot->AddMember(field).String(
        std::string(static_cast<const char *>(data), len));
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}
ydb_status_t ydb_params_add_member_null(YdbParamBuilder *b, const char *field,
                                        YdbResultDetails *rd) {
  CHECK_RD(rd);
  if (!b || !field) {
    return RD(YDB_ERR_BAD_REQUEST, "invalid null member");
  }
  try {
    b->slot->AddMember(field).EmptyOptional();
    return YDB_OK;
  }
  CATCH_ALL_STATUS();
}

static int ydb_resultsets_count(const YdbResultSets *rs, YdbResultDetails *rd) {
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

ydb_type_t ydb_resultset_column_type(const YdbResultSet *rs, int col_index,
                                     YdbResultDetails *rd) {
  try {
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
  CATCH_ALL_INT(rd, YDB_TYPE_UNKNOWN);
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
